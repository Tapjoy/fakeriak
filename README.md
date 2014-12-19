# fakeriak

An in-memory hash implementation of Riak.  This is designed to be used in specs
to remove the external dependency of a running Riak server.

## Features

The following Riak features are currently not implemented:
* Solr search queries
* Map/Reduce with Erlang (Javascript is supported)

The following Riak features are deprecated in Riak 1.x, removed in Riak 2.x,
and therefore will not be supported:
* Luwak file storage
* Link walking

Everything else is fully supported.

## Usage

On Riak 1.x:

```ruby
riak = Riak::Client.new(:nodes => ["127.0.0.1"], :protobuffs_backend => :Memory, :http_backend => :Memory)
```

On Riak 2.x:

```ruby
riak = Riak::Client.new(:nodes => ["127.0.0.1"], :protobuffs_backend => :Memory)
```

To reset all the data:

```ruby
riak.buckets.each do |bucket|
  bucket.keys.each {|key| bucket.delete(key)}
end
```

### Within RSpec:

Typically this gem is used within specs in order to avoid external dependencies
when running the spec suite.  In order to ensure that the data is reset in each
spec, you'll need to explicitly do so as part of a `before` or `after` hook in
the spec helper.  For example:

```ruby
$riak = Riak::Client.new(:nodes => ["127.0.0.1"], :protobuffs_backend => :Memory)

RSpec.configure do |config|
  # ...

  config.before(:each) do
    $riak.buckets.each do |bucket|
      bucket.keys.each {|key| bucket.delete(key)}
    end
  end
end
```

## Notes

### Server data

One important note that is that data will be shared across servers that are
configured in the same client.  For example, support you configured your
client like so:

```ruby
riak = Riak::Client.new(:nodes => ["10.0.0.1", "10.0.0.2"], :protobuffs_backend => :Memory)
```

This would force data to be consistent across both the `10.0.0.1` node and the
`10.0.0.2` node.  As a result, creating a client with only one of those nodes
would return the same results.

## Testing

To test, you should use [appraisal](https://github.com/thoughtbot/appraisal) like so:

```
bundle exec appraisal riak-1.4 rspec
bundle exec appraisal riak-2.0 rspec
bundle exec appraisal riak-2.1 rspec
```

## Dependencies

fakeriak has been tested and verified with with the following dependencies:

* Ruby: 1.9.3+
* Riak Client: 1.4+
* Riak Server: 1.4+