require 'hyperdex'
require 'eventmachine'

module EM; end  #:nodoc:
module EM::HyperDex; end  #:nodoc:

# An EventMachine-enabled client for [HyperDex](http://hyperdex.org/).
#
# This is a fairly straightforward async-friendly interface to the hyperdex
# NoSQL data store.  All of the normal (synchronous) methods that you are
# used to using are available, except that they're automatically async.
# Schweeeeeet.
#
# Using it is quite simple (for an EM client, anyway...).  You just create
# an instance of `EM::HyperDex::Client`, and then call (almost) any of the
# standard HyperDex methods you know and love against it, with the same
# arguments.  The only difference is that you can pass a block to the
# method, indicating what you want to do once the request is complete, and
# the method returns a Deferrable which you can define callbacks and
# errbacks.  The callback will be passed whatever the HyperDex data method
# would ordinarily return, in a synchronous world.
#
# Searching methods ({#search}, {#sorted_search}) work *slightly*
# differently.  Instead of the entire resultset being passed back in a
# single callback, the callback is executed *after* the result set has been
# processed.  Each item in the search result is accessed via
# {EnumerableDeferrable#each} on the deferrable that is returned from the
# {#search} method.  See the docs for {#search} and {#sorted_search} for
# examples of what this looks like.
#
class EM::HyperDex::Client
	# Create a new `EM::HyperDex::Client` for you to play with.
	#
	# This method does **not** take a callback block; the client is created
	# synchronously.  However, this isn't the problem you might otherwise
	# expect it to be, because initializing a client doesn't make any network
	# connections or otherwise potentially-blocking calls; rather, it simply
	# initializes some data structures and then returns.
	#
	# @param host [String] A hostname or IP address (v4 or v6) of a coordinator
	#   within the cluster to make initial contact.
	#
	# @param port [Integer] The port of the coordinator you wish to contact.
	#
	def initialize(host = 'localhost', port = 1982)
		@client = HyperDex::Client::Client.new(host, port)
		@failed = false
		@outstanding = {}
	end

	ASYNC_METHODS = HyperDex::Client::Client.
	                  instance_methods.
	                  map(&:to_s).
	                  grep(/^async_/).
	                  map { |m| m.gsub(/^async_/, '') }
	private_constant :ASYNC_METHODS

	ASYNC_METHODS.each do |m|
		hyperdex_method = "async_#{m}"

		define_method(m) do |*args, &block|
			if @failed
				return failed_deferrable("This client has failed.  Please open a new one.")
			end

			df = ::EM::Completion.new
			df.callback(&block) if block

			begin
				if ::EM.reactor_running?
					add_outstanding(@client.__send__(hyperdex_method, *args), df)
				else
					df.succeed(@client.__send__(m, *args))
				end
			rescue HyperDex::Client::HyperDexClientException => ex
				df.fail(ex)
			end

			df
		end
	end

	ITERATOR_METHODS = %w{search sorted_search}
	private_constant :ITERATOR_METHODS

	# @!macro search_params
	#   @param spacename [#to_s] The name of the hyperdex space to search
	#
	#   @param predicates [Hash<#to_s, HyperDex::Client::Predicate>] A
	#     collection of predicates to apply to the search.
	#
	#   @return [EnumerableDeferrable] a deferrable which responds to
	#     {EnumerableDeferrable#each each} to return the search results.
	#   
	#
	# @!method search(spacename, predicates)
	#
	# Perform a search for all objects in the specified space that match the
	# predicates provided.
	#
	# @macro search_params
	#
	# @example iterating through search results
	#   c = EM::HyperDex::Client.new
	#   c.search(:test, :towels => HyperDex::Client::GreaterThan.new(42)).each do |r|
	#     puts "This is an object with a high towel count: #{r.inspect}"
	#   end.callback do
	#     puts "Towel search complete"
	#   end.errback do |ex|
	#     puts "Towel search failed: #{ex.class}: #{ex.message}"
	#   end
	#

	# @!method sorted_search(spacename, predicates, sortby, limit, maxmin)
	#
	# Perform a search for all objects in the specified space that match
	# the predicates provided, sorting the results and optionally returning
	# only a subset of the results.
	#
	# @macro search_params
	#
	# @param sortby [#to_s] the attribute to sort the results by
	#
	# @param limit [Integer] the maximum number of results to return
	#
	# @param maxmin [String] Maximize (`"max"`) or minimize (`"min"`)
	#   (I shit you not, that's what the upstream docs say)
	#
	# @todo Document the `maxmin` parameter *properly*.
	#
	# @see #search
	#
	ITERATOR_METHODS.each do |m|
		define_method(m) do |*args, &block|
			if @failed
				return failed_deferrable("This client has failed.  Please open a new one.")
			end

			begin
				iter = @client.__send__(m, *args)
			rescue StandardError => ex
				return EM::Completion.new.tap do |df|
					df.fail(ex)
				end
			end

			df = EnumerableDeferrable.new(iter)
			df.callback(&block) if block

			if ::EM.reactor_running?
				add_outstanding(iter, df)
			else
				begin
					until (item = iter.next).nil?
						df.item_available(item)
					end
					df.item_available(nil)
				rescue Exception => ex
					df.fail(ex)
				end
			end

			df
		end
	end

	# Callback from the `EM.watch` system, to poke us when a response is
	# ready to be processed.
	#
	# @api private
	#
	def handle_response
		begin
			op = @client.loop(0)
		rescue HyperDex::Client::HyperDexClientException
			# Something has gone wrong, and we're just going to take our bat
			# and ball and go home.
			@outstanding.values.each { |op| op.fail(ex) }
			@outstanding = {}

			# Call a dummy get_outstanding so EM stops watching the socket
			get_outstanding(nil)

			@failed = true
			return
		end

		# It's possible for the client's poll_fd to see activity when there
		# isn't any new completed operation; according to rescrv, this can
		# happen "because of background activity".  In that case, `#loop`
		# called with a timeout will return `nil`, and we should just
		# return quietly.
		if op.nil?
			return
		end

		df = get_outstanding(op)

		begin
			if df.respond_to?(:item_available)
				df.item_available
				# Put the deferrable back in the outstanding ops hash if
				# there's more items to go
				add_outstanding(op, df) unless df.completed?
			else
				df.succeed(op.wait)
			end
		rescue HyperDex::Client::HyperDexClientException => ex
			df.fail(ex)
		end
	end

	# Associate an in-progress operation with the deferrable that will be
	# completed when the operation completes.  Also handles telling EM to
	# start watching the client's `poll_fd` if necessary.
	#
	def add_outstanding(op, df)
		# If we don't have any operations already in progress, then
		# we aren't watching the poll_fd, so we probably want to start
		# doing that now
		if @outstanding.empty?
			@em_conn = ::EM.watch(@client.poll_fd, Watcher, self)
			@em_conn.notify_readable = true
		end

		@outstanding[op] = df
	end

	# Retrieve the deferrable associated with the specified operation, if
	# one exists.  Also handles telling EM to stop watching the `poll_fd`,
	# if there are now no more outstanding operations.
	def get_outstanding(op)
		@outstanding.delete(op).tap do
			if @outstanding.empty?
				@em_conn.detach
				@em_conn = nil
			end
		end
	end

	private
	# Return a new deferrable that has failed with a `RuntimeError` with the
	# given message.
	#
	def failed_deferrable(msg)
		::EM::Completion.new.tap do |df|
			df.fail(RuntimeError.new(msg))
		end
	end

	# Mix-in module for EM.watch.
	#
	# @api private
	#
	module Watcher
		# Create the watcher.
		#
		# @api private
		def initialize(em_client)
			@em_client = em_client
		end

		# Handle the fact that more data is available.
		#
		# @api private
		def notify_readable
			@em_client.handle_response
		end
	end

	# A deferrable that can be enumerated.
	#
	# This is a really *freaky* kind of a deferrable.  It accepts the usual
	# `callback` and `errback` blocks, but it *also* has a special {#each}
	# callback, which will cause the block provided for each item in the
	# search result set.  Once the result set has been enumerated, the
	# `callback` will be called.
	#
	# @todo Define a set of other Enumerable methods that can be usefully
	#   used asynchronously (if there are any), like perhaps `#map`,
	#   `#inject`, etc.
	#
	class EnumerableDeferrable < EM::Completion
		# Create a new EnumerableDeferrable
		#
		# @param iter [HyperDex::Client::Iterator] What to call `#next` on
		#   to get the next item.
		#
		# @api private
		#
		def initialize(iter)
			@iter = iter
			@items = []
			super()
		end

		# Define a block to call for each item in the result set.
		#
		# @yield the next available item in the result set
		#
		# @yieldparam item [Hash<Symbol, Object>] the hyperdex value object
		#
		def each(&blk)
			return Enumeration.new(self) unless block_given?

			if state == :succeeded
				@items.each { |i| blk.call(i) }
			else
				@each_block = blk
			end

			self
		end

		# Trigger whenever we know there's an item available.
		#
		# @api private
		#
		def item_available(val = NoValue)
			if val == NoValue
				val = @iter.next
			end

			if val.nil?
				self.succeed
			else
				if @each_block
					begin
						@each_block.call(val)
					rescue Exception => ex
						fail(ex)
					end
				else
					@items << val
				end
			end
		end

		NoValue = Object.new
		private_constant :NoValue
	end
end
