require 'hyperdex'
require 'eventmachine'

module EM; end
module EM::HyperDex; end

class EM::HyperDex::Client
	def initialize(host, port)
		@client = HyperDex::Client::Client.new(host, port)
		@failed = false
		@outstanding = {}

		if ::EM.reactor_running?
			@em_conn = ::EM.watch(@client.poll_fd, Watcher, self)
			@em_conn.notify_readable = true
		end
	end

	ASYNC_METHODS = HyperDex::Client::Client.
	                  instance_methods.
	                  map(&:to_s).
	                  grep(/^async_/).
	                  map { |m| m.gsub(/^async_/, '') }

	ASYNC_METHODS.each do |m|
		hyperdex_method = "async_#{m}"

		define_method(m) do |*args, &block|
			df = ::EM::DefaultDeferrable.new

			if @failed
				return df.tap do |df|
					df.fail(RuntimeError.new("This client has failed.  Please open a new one."))
				end
			end

			begin
				if ::EM.reactor_running?
					@outstanding[@client.__send__(hyperdex_method, *args)] = df
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

	ITERATOR_METHODS.each do |m|
		define_method(m) do |*args, &block|
			if @failed
				return ::EM::DefaultDeferrable.new.tap do |df|
					df.fail(RuntimeError.new("This client has failed.  Please open a new one."))
				end
			end

			iter = @client.__send__(m, *args)
			df = DeferrableEnumerable.new(iter)
			df.callback(&block) if block_given?

			begin
				@outstanding[iter] = df
			rescue HyperDex::Client::HyperDexClientException => ex
				::EM::DefaultDeferrable.new.fail(ex)
			end
		end
	end

	def handle_response
		begin
			df = @outstanding.delete(op = @client.loop(0))
		rescue HyperDex::Client::HyperDexClientException
			# Something has gone wrong, and we're just going to take our bat
			# and ball and go home.
			@outstanding.values.each { |op| op.fail(ex) }
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

		begin
			if df.respond_to?(:item_available)
				df.item_available
			else
				df.succeed(op.wait)
			end
		rescue HyperDex::Client::HyperDexClientException => ex
			df.fail(ex)
		end
	end

	def close
		@outstanding.each { |o| o.wait }
		@em_conn.detach
		@client = nil
	end

	module Watcher
		def initialize(em_client)
			@em_client = em_client
		end

		def notify_readable
			@em_client.handle_response
		end
	end

	class DeferrableEnumerable
		include Enumerable
		include ::EM::Deferrable

		def initialize(iter)
			@iter = iter
		end

		def each(&blk)
			return self unless block_given?

			@each_block = blk
		end

		def item_available
			val = @iter.next
			if val.nil?
				succeed
			else
				begin
					@each_block.call(val)
				rescue Exception => ex
					fail(ex)
				end
			end
		end
	end
end
