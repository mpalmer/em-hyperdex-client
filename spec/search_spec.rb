require_relative './spec_helper'

describe "#search" do
	let(:client) { EM::HyperDex::Client.new }

	it "works when standalone" do
		expect(mock_client).
		  to receive(:search).
		  with(:foo, 42).
		  and_return(mock_iter = double(HyperDex::Client::Iterator))

		resultset = %w{alpha beta gamma delta}

		expect(mock_iter).
		  to receive(:next).
		  with(no_args).
		  and_return(*(resultset + [nil]))

		client.search(:foo, 42).each do |v|
			expect(v).to eq(resultset.shift)
		end.errback do
			# Derpy way of saying "this should not happen"
			expect(false).to be(true)
		end

		expect(resultset).to be_empty
	end

	it "works inside EM" do
		resultset = %w{alpha beta gamma delta}
		in_em do
			expect_iter(:search, [:foo, 42], resultset)

			df = client.search(:foo, 42).each do |v|
				expect(v).to eq(resultset.shift)
			end.callback do
				EM.stop
			end.errback do
				# This should not happen
				expect(false).to be(true)
			end

			expect(df).to be_a(EM::HyperDex::Client::EnumerableDeferrable)
		end

		expect(resultset).to be_empty
	end
end
