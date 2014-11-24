require_relative './spec_helper'

describe "#get" do
	let(:client) { EM::HyperDex::Client.new }

	it "works when standalone" do
		expect(mock_client).
		  to receive(:get).
		  with(:foo, 42).
		  and_return("towels")

		client.get(:foo, 42).callback do |v|
			expect(v).to eq("towels")
		end
	end

	it "works inside EM" do
		in_em do
			expect_op(:get, [:foo, 42], "towels")

			df = client.get(:foo, 42) do |v|
				expect(v).to eq("towels")
				EM.stop
			end

			expect(df).to be_a(::EM::Completion)
		end
	end

	it "works multiple times" do
		in_em do
			expect_op(:get, [:foo, 42], "towels")
			expect_op(:get, [:bar, "baz"], "wombat")

			client.get(:foo, 42) do |v|
				expect(v).to eq("towels")
				client.get(:bar, "baz") do |v|
					expect(v).to eq("wombat")
					EM.stop
				end
			end
		end
	end

	it "works in parallel" do
		in_em do
			expect_op(:get, [:foo, 42], "towels")
			expect_op(:get, [:bar, "baz"], "wombat", 0.1)
			op_count = 2

			client.get(:foo, 42) do |v|
				expect(v).to eq("towels")
				op_count -= 1
			end

			client.get(:bar, "baz") do |v|
				expect(v).to eq("wombat")
				op_count -= 1
			end

			EM.add_periodic_timer(0.0001) do
				EM.stop if op_count == 0
			end
		end
	end

	it "works with out-of-order responses" do
		in_em do
			expect_op(:get, [:foo, 42], "towels", 0.1)
			expect_op(:get, [:bar, "baz"], "wombat")
			op_count = 2

			client.get(:foo, 42) do |v|
				expect(v).to eq("towels")
				op_count -= 1
			end

			client.get(:bar, "baz") do |v|
				expect(v).to eq("wombat")
				op_count -= 1
			end

			EM.add_periodic_timer(0.0001) do
				EM.stop if op_count == 0
			end
		end
	end
end
