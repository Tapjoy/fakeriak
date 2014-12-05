# fakeriak

An in-memory hash implementation of Riak.

## Usage

On Riak 1.x:

```ruby
riak = Riak::Client.new(:protocol => :pbc, :nodes => [...], :protobuffs_backend => :Memory, :http_backend => :Memory)
```

On Riak 2.x:

```ruby
riak = Riak::Client.new(:nodes => [...], :protobuffs_backend => :Memory)
```

## Feature parity

The following Riak features are currently not supported:
* Luwak (deprecated in Riak 1.x, removed in Riak 2.x)
* Link walking (deprecated in Riak 1.x, removed in Riak 2.x)
* Map reduce
* Search queries

Everything else is fully supported.

## Tasks needed to open source:

1. Add documentation to code
2. Add specs