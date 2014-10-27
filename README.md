An [EventMachine](http://rubyeventmachine.com) interface to the
[HyperDex](http://hyperdex.org) NoSQL data store.


# Installation

It's a gem:

    gem install em-hyperdex-client

If you're the sturdy type that likes to run from git:

    rake build; gem install pkg/em-hyperdex-client-<whatever>.gem

Or, if you've eschewed the convenience of Rubygems, then you presumably know
what to do already.


# Usage

To use any of these methods, you will want to add the following require:

    require 'em-hyperdex-client'

Somewhere inside your `EM.run` block, create a client object:

    c = EM::HyperDex::Client.new('localhost', 1982)

Then, call any (synchronous) method you normally would on a regular
`HyperDex::Client::Client` instance, but instead of having the result
returned to you, you use `#callback` to handle the response:

    c.get(:kv, "foo").callback do |r|
      puts "The value of foo is #{r}"
    end

To specify the proper order of operations, you need to nest your callbacks:

    c.get(:kv, "foo").callback do |r|
      c.put(:kv, "foo", r.merge(:baz => "wombat")).callback do
        c.get(:kv, "bar").callback do |r|
          puts "Ohai!"
        end
      end
    end

To handle errors, you specify an `#errback` callback:

    c.get(:kv, "foo").callback do |r|
      c.put(:kv, "foo", r.merge(:baz => "wombat")).callback do
        puts "Completed successfully"
      end.errback do |ex|
        puts "Error on put: #{ex.message}"
      end
    end.errback do |ex|
      puts "Error on get: #{ex.message}"
    end

What gets yielded to your `#errback` block is the same exception that would
otherwise have been raised by the operation you performed -- typically,
that'll be a `HyperDex::Client::HyperDexClientException`.


# Contributing

Bug reports should be sent to the [Github issue
tracker](https://github.com/mpalmer/em-hyperdex-client/issues), or
[e-mailed](mailto:theshed+em-hyperdex-client@hezmatt.org).  Patches can be
sent as a Github pull request, or
[e-mailed](mailto:theshed+em-hyperdex-client@hezmatt.org).
