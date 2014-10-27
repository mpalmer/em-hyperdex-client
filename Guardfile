guard 'spork', :rspec_port => 19203 do
  watch('Gemfile')             { :rspec }
  watch('Gemfile.lock')        { :rspec }
  watch('spec/spec_helper.rb') { :rspec }
end

guard 'rspec',
      :cmd            => "rspec --drb --drb-port 19203",
      :all_on_start   => true,
      :all_after_pass => true do
  watch(%r{^spec/.+_spec\.rb$})
  watch(%r{^lib/})               { "spec" }
end

