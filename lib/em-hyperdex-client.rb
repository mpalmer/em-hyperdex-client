require 'hyperdex'
require 'eventmachine'

module EM; end
module EM::HyperDex; end

class EM::HyperDex::Client
	def initialize(host, port)
		@client = HyperDex::Client::Client.new(host, port)
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

	ITERATOR_METHODS = %w{search sorted_search}

	(ASYNC_METHODS + ITERATOR_METHODS).each do |m|
		hyperdex_method = ITERATOR_METHODS.include?(m) ?
		                    m :
		                    "async_#{m}"

		class_eval <<-EOD, __FILE__, __LINE__
			def #{m}(*args)
				df = ::EM::DefaultDeferrable.new

				begin
					if ::EM.reactor_running?
						@outstanding[@client.__send__(#{hyperdex_method}, *args)] = df
					else
						df.succeed(@client.__send__(m, *args))
					end
				rescue HyperDex::Client::HyperDexClientException => ex
					df.fail(ex)
				end

				df
			end
		EOD
	end

	def handle_response
		begin
			df = @outstanding.delete(op = @client.loop)
			df.succeed(resp.wait)
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
end
