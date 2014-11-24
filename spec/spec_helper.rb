require 'spork'

Spork.prefork do
	require 'bundler'
	Bundler.setup(:default, :test)
	require 'rspec/core'
	require 'rspec/mocks'

	require 'pry'

	module EventMachineTestHelper
		def in_em(timeout = 1)
			Timeout::timeout(timeout) do
				::EM.run do
					yield
				end
			end
		rescue Timeout::Error
			EM.stop
			raise RuntimeError,
			      "EM didn't finish before the timeout"
		end

		def expect_op(op, args, rv, response_delay = 0.0001)
			expect(mock_client).
			  to receive("async_#{op}".to_sym).
			  with(*args).
			  and_return(mock_op = double(HyperDex::Client::Deferred))

			expect(mock_op).
			  to receive(:wait).
			  with(no_args).
			  and_return(rv)

			::EM.add_timer(response_delay) do
				expect(mock_client).
				  to receive(:loop).
				  with(0).
				  and_return(mock_op)

				client.__send__(:handle_response)
			end
		end

		def expect_iter(op, args, results, response_delay = 0.0001)
			expect(mock_client).
			  to receive(op).
			  with(*args).
			  and_return(mock_iter = double(HyperDex::Client::Iterator))

			expect(mock_iter).
			  to receive(:next).
			  with(no_args).
			  and_return(*(results + [nil]))

			(results.length + 1).times do |i|
				::EM.add_timer(response_delay * (i+1)) do
					expect(mock_client).
					  to receive(:loop).
					  with(0).
					  and_return(mock_iter)

					client.__send__(:handle_response)
				end
			end
		end

		def mock_client(host = 'localhost', port = 1982)
			@mock_client ||= begin
				expect(HyperDex::Client::Client).
				  to receive(:new).
				  with(host, port).
				  and_return(mock = double(HyperDex::Client::Client))

				allow(mock).
				  to receive(:poll_fd).
				  with(no_args).
				  and_return(0)
				mock
			end
		end
	end

	RSpec.configure do |config|
		config.fail_fast = true
#		config.full_backtrace = true

		config.include EventMachineTestHelper

		config.expect_with :rspec do |c|
			c.syntax = :expect
		end
	end
end

Spork.each_run do
	require 'em-hyperdex-client'
end
