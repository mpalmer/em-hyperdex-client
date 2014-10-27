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

	def method_missing(m, *args)
		unless @client.respond_to?("async_#{m}")
			return super
		end

		df = ::EM::DefaultDeferrable.new

		begin
			if ::EM.reactor_running?
				@outstanding[@client.__send__("async_#{m}", *args)] = df
			else
				df.succeed(@client.__send__(m, *args))
			end
		rescue HyperDex::Client::HyperDexClientException => ex
			df.fail(ex)
		end

		df
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
