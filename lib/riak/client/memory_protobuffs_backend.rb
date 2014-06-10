require 'riak'
require 'riak/failed_request'

module Riak
  class Client
    class MemoryProtobuffsBackend
      class << self
        attr_reader :server_data

        def configured?
          true
        end
      end

      @server_data = {}

      attr_accessor :client
      attr_accessor :node
      def initialize(client, node)
        @client = client
        @node = node
        @data = self.class.server_data[node.host] || begin
          data = {}
          client.nodes.each do |node|
            self.class.server_data[node.host] = data
          end
          data
        end
      end

      def ping
        # no-op
      end

      def get_client_id
        @client_id
      end

      def set_client_id(id)
        @client_id = id
      end

      def server_info
        {}
      end

      def fetch_object(bucket, key, options={})
        result = data(bucket)[key]
        raise ::Riak::ProtobuffsFailedRequest.new(:not_found, 'not found') unless result
        result
      end

      def reload_object(robject, options={})
        fetch_object(robject.bucket, robject.key, options)
      end

      def store_object(robject, options={})
        data(robject.bucket)[robject.key] = robject
      end

      def delete_object(bucket, key, options={})
        data(bucket).delete(key)
      end

      def get_counter(bucket, key, options={})
        raise NotImplementedError
      end

      def post_counter(bucket, key, amount, options={})
        raise NotImplementedError
      end

      def get_bucket_props(bucket)
        raise NotImplementedError
      end

      def set_bucket_props(bucket, props)
        raise NotImplementedError
      end

      def reset_bucket_props(bucket)
        raise NotImplementedError
      end

      def list_keys(bucket, options={}, &block)
        data(bucket).keys
      end

      def list_buckets(options={}, &blk)
        @data.keys
      end

      def mapred(mr, &block)
        raise NotImplementedError
      end

      def get_index(bucket, index, query, query_options={}, &block)
        raise NotImplementedError
      end

      def search(index, query, options={})
        raise NotImplementedError
      end

      def create_search_index(name, schema=nil, n_val=nil)
        raise NotImplementedError
      end

      def get_search_index(name)
        raise NotImplementedError
      end

      def delete_search_index(name)
        raise NotImplementedError
      end

      def create_search_schema(name, content)
        raise NotImplementedError
      end

      def get_search_schema(name)
        raise NotImplementedError
      end

      def teardown
      end

      def socket
        nil
      end

      private
      def data(bucket)
        @data[bucket.name] ||= {}
      end
    end
  end
end