require 'securerandom'

module Riak
  class Client
    class MemoryBackend
      # Loads, and deserializes CRDTs from Riak into Ruby hashes, sets, strings,
      # and integers.
      class CrdtLoader
        attr_reader :backend
        attr_reader :context

        # The default values for each CRDT type
        DEFAULTS = {
          :set => lambda { Set.new },
          :counter => lambda { 0 },
          :map => lambda { {:counters => {}, :flags => {}, :maps => {}, :registers => {}, :sets => {}} }
        }

        def initialize(backend)
          @backend = backend

          # Use a generated context since they're meaningless in a memory backend
          @context = SecureRandom.uuid
        end

        # Returns the deserialized CRDT for the given key
        def load(bucket, key, bucket_type, options = {})
          begin
            robject = backend.fetch_object(bucket, key, :type => bucket_type)
            Marshal.load(robject.raw_data)
          rescue Riak::ProtobuffsFailedRequest => ex
            # Key doesn't exist in the data store: provide a default initial
            # value based on the data type associated with the provided
            # bucket type
            datatype = backend.get_bucket_type_props(bucket_type)['datatype'].to_sym

            if DEFAULTS[datatype]
              DEFAULTS[datatype].call
            else
              raise Riak::ProtobuffsErrorResponse, "Unsupported CRDT data type: #{datatype}"
            end
          end
        end

        # Gets the class that is able to interpret and load the given value into
        # its Ruby equivalent.  In the memory backend, values are already loaded
        # -- so this is essentially a no-op.
        def get_loader_for_value(value)
          value
        end
      end
    end
  end
end