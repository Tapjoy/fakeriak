require 'securerandom'
begin
  require 'riak/errors/crdt_error'
rescue LoadError => ex
  # No-op
end

module Riak
  class Client
    class MemoryBackend
      # Serializes and writes CRDT operations from {Riak::Crdt::Operation} members
      # to a Riak cluster.
      class CrdtOperator
        # Represents the result of an operation
        class Result < Struct.new(:key, :value, :context)
          alias_method :rubyfy, :value
        end

        attr_reader :backend
        
        def initialize(backend) #:nodoc:
          @backend = backend
        end

        # Processes the given operations
        def operate(bucket, key, bucket_type, operation, options = {})
          bucket = backend.client.bucket(bucket) unless bucket.is_a?(Bucket)
          operations = Array(operation)

          # Get the current data for this key
          data = backend.crdt_loader.load(bucket, key, bucket_type)
          datatype = operations[0].type

          # Process each operation on the data
          operations.each do |operation|
            data = merge(data, datatype, operation.value)
          end

          # Store the value for future access
          robject = RObject.new(bucket, key)
          robject.raw_data = Marshal.dump(data)
          robject.content_type = "application/riak_#{datatype}"
          robject.store(:type => bucket_type)

          # Wrap the result in an API compatible with Riak
          Result.new(key, data, SecureRandom.uuid)
        end

        private
        # Merges in the value to the given data
        def merge(data, datatype, value)
          data ||= CrdtLoader::DEFAULTS[datatype].call if CrdtLoader::DEFAULTS[datatype]

          case datatype
          when :set
            data.merge(Array(value[:add]))
            data.subtract(Array(value[:remove]))
          when :counter
            data += value
          when :flag, :register
            data = value
          when :map
            datatype = value.type
            key = value.name

            if value.is_a?(Crdt::Operation::Update)
              value = value.value

              # Recursively merge in data
              data[:"#{datatype}s"][key] = merge(data[:"#{datatype}s"][key], datatype, value)
            else
              data[:"#{datatype}s"].delete(key)
            end
          end

          data
        end
      end
    end
  end
end