# fakeriak

An in-memory hash implementation of Riak.  This is designed to be used in specs
to remove the external dependency of a running Riak server.

## Feature parity

The following Riak features are currently not implemented:
* Solr search queries

The following Riak features are deprecated in Riak 1.x, removed in Riak 2.x,
and therefore will not be supported:
* Luwak file storage
* Link walking

Everything else is fully supported.

## Usage

On Riak 1.x:

```ruby
riak = Riak::Client.new(:nodes => [...], :protobuffs_backend => :Memory, :http_backend => :Memory)
```

On Riak 2.x:

```ruby
riak = Riak::Client.new(:nodes => [...], :protobuffs_backend => :Memory)
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
spec, you'll need to explicitly do so as part of a `before` or `after` callback
in the spec helper.  For example:

```ruby
RSpec.configure do |config|
  # ...

  config.before(:each) do
    riak.buckets.each do |bucket|
      bucket.keys.each {|key| bucket.delete(key)}
    end
  end
end
```

## Server data

One important note that is that data will be shared across 