require 'riak'
require 'riak/util/translation'
require 'riak/client/beefcake/messages'
begin
  # Riak 2.x
  require 'riak/errors/failed_request'
rescue LoadError
  # Riak 1.x
  require 'riak/failed_request'
end

require 'base64'
require 'digest/md5'
require 'multi_json'
require 'execjs'

module Riak
  class Client
    # An in-memory hash implementation of Riak
    class MemoryBackend
      class << self
        # Data associated with each known server
        attr_reader :server_data

        # Whether the backend has been properly configured
        def configured?
          true
        end
      end

      include Util::Translation

      # The data associated with all known Riak servers
      @server_data = {}

      # The default properties for a bucket
      DEFAULT_BUCKET_PROPS = {
        "n_val" => 3,
        "precommit" => [], "has_precommit" => true,
        "postcommit" => [], "has_postcommit" => true,
        "chash_keyfun" => {"mod" => "riak_core_util", "fun" => "chash_std_keyfun"},
        "linkfun" => {"mod" => "riak_kv_wm_link_walker", "fun" => "mapreduce_linkfun"},
        "old_vclock" => 86400, "young_vclock" => 20,
        "big_vclock" => 50, "small_vclock" => 50,
        "r" => "quorum", "w" => "quorum",
        "dw" => "quorum", "rw" => "quorum",
        "pr" => 0, "pw" => 0,
        "notfound_ok" => true
      }

      # The default stats for Riak
      DEFAULT_STATS = {
        "basho_stats_version" => "1.0.3", "bitcask_version" => "1.6.6-0-g230b6d6", "cluster_info_version" => "1.2.4", "compiler_version" => "4.8.1",
        "crypto_version" => "2.1", "erlang_js_version" => "1.2.2", "erlydtl_version" => "0.7.0", "goldrush_version" => "0.1.5",
        "inets_version" => "5.9", "kernel_version" => "2.15.1", "lager_version" => "2.0.1", "merge_index_version" => "1.3.2-0-gcb38ee7",
        "mochiweb_version" => "1.5.1p6", "os_mon_version" => "2.2.9", "public_key_version" => "0.15", "riak_api_version" => "1.4.10-0-gc407ac0",
        "riak_control_version" => "1.4.10-0-g73c43c3", "riak_core_version" => "1.4.10", "riak_kv_version" => "1.4.10-0-g64b6ad8", "riak_pipe_version" => "1.4.10-0-g9353526",
        "riak_search_version" => "1.4.10-0-g6e548e7", "riak_sysmon_version" => "1.1.3", "runtime_tools_version" => "1.8.8", "sasl_version" => "2.2.1",
        "sidejob_version" => "0.2.0", "ssl_version" => "5.0.1", "stdlib_version" => "1.18.1", "syntax_tools_version" => "1.6.8",
        "sys_driver_version" => "2.0", "webmachine_version" => "1.10.4-0-gfcff795",

        "connected_nodes" => [],
        "converge_delay_last" => 0, "converge_delay_max" => 0, "converge_delay_mean" => 0, "converge_delay_min" => 0,
        "coord_redirs_total" => 0,
        "cpu_avg1" => 0, "cpu_avg15" => 0, "cpu_avg5" => 0, "cpu_nprocs" => 0,
        "dropped_vnode_requests_total" => 0,
        "executing_mappers" => 0,
        "gossip_received" => 0,
        "handoff_timeouts" => 0,
        "ignored_gossip_total" => 0,
        "index_fsm_active" => 0, "index_fsm_create" => 0, "index_fsm_create_error" => 0,
        "leveldb_read_block_error" => "undefined",
        "list_fsm_active" => 0, "list_fsm_create" => 0, "list_fsm_create_error" => 0,
        "mem_allocated" => 0, "mem_total" => 0,
        "memory_atom" => 0, "memory_atom_used" => 0, "memory_binary" => 0, "memory_code" => 0, "memory_ets" => 0,
        "memory_processes" => 0, "memory_processes_used" => 0, "memory_system" => 0, "memory_total" => 0,
        "node_get_fsm_active" => 0, "node_get_fsm_active_60s" => 0,
        "node_get_fsm_objsize_100" => 0, "node_get_fsm_objsize_95" => 0, "node_get_fsm_objsize_99" => 0, "node_get_fsm_objsize_mean" => 0, "node_get_fsm_objsize_median" => 0,
        "node_get_fsm_in_rate" => 0, "node_get_fsm_out_rate" => 0,
        "node_get_fsm_rejected" => 0, "node_get_fsm_rejected_60s" => 0, "node_get_fsm_rejected_total" => 0,
        "node_get_fsm_siblings_100" => 0, "node_get_fsm_siblings_95" => 0, "node_get_fsm_siblings_99" => 0, "node_get_fsm_siblings_mean" => 0, "node_get_fsm_siblings_median" => 0,
        "node_get_fsm_time_100" => 0, "node_get_fsm_time_95" => 0, "node_get_fsm_time_99" => 0, "node_get_fsm_time_mean" => 0, "node_get_fsm_time_median" => 0,
        "node_gets" => 0, "node_gets_total" => 0,
        "node_put_fsm_active" => 0, "node_put_fsm_active_60s" => 0,
        "node_put_fsm_in_rate" => 0, "node_put_fsm_out_rate" => 0,
        "node_put_fsm_rejected" => 0, "node_put_fsm_rejected_60s" => 0, "node_put_fsm_rejected_total" => 0,
        "node_put_fsm_time_100" => 0, "node_put_fsm_time_95" => 0, "node_put_fsm_time_99" => 0, "node_put_fsm_time_mean" => 0, "node_put_fsm_time_median" => 0,
        "node_puts" => 0, "node_puts_total" => 0,
        "nodename" => "riak@127.0.0.1",
        "pbc_active" => 1, "pbc_connects" => 0, "pbc_connects_total" => 0,
        "pipeline_active" => 0, "pipeline_create_count" => 0, "pipeline_create_error_count" => 0, "pipeline_create_error_one" => 0, "pipeline_create_one" => 0,
        "postcommit_fail" => 0, "precommit_fail" => 0,
        "read_repairs" => 0, "read_repairs_total" => 0,
        "rebalance_delay_last" => 0, "rebalance_delay_max" => 0, "rebalance_delay_mean" => 0, "rebalance_delay_min" => 0,
        "rejected_handoffs" => 0,
        "riak_core_stat_ts" => 1416840306,
        "riak_kv_stat_ts" => 1416840306,
        "riak_kv_vnodeq_max" => 0, "riak_kv_vnodeq_mean" => 0, "riak_kv_vnodeq_median" => 0, "riak_kv_vnodeq_min" => 0, "riak_kv_vnodeq_total" => 0, "riak_kv_vnodes_running" => 64,
        "riak_pipe_stat_ts" => 1416840305,
        "riak_pipe_vnodeq_max" => 0, "riak_pipe_vnodeq_mean" => 0, "riak_pipe_vnodeq_median" => 0, "riak_pipe_vnodeq_min" => 0, "riak_pipe_vnodeq_total" => 0, "riak_pipe_vnodes_running" => 64,
        "ring_creation_size" => 64, "ring_members" => ["riak@127.0.0.1"], "ring_num_partitions" => 64, "ring_ownership" => "[{'riak@127.0.0.1',64}]",
        "rings_reconciled" => 0, "rings_reconciled_total" => 0,
        "storage_backend" => "riak_kv_bitcask_backend",
        "sys_global_heaps_size" => 0, "sys_heap_type" => "private", "sys_logical_processors" => 8, "sys_otp_release" => "R15B01",
        "sys_process_count" => 1900, "sys_smp_support" => true, "sys_system_architecture" => "x86_64-unknown-linux-gnu", "sys_system_version" => 0,
        "sys_thread_pool_size" => 64, "sys_threads_enabled" => true, "sys_wordsize" => 8,
        "vnode_gets" => 0, "vnode_gets_total" => 0,
        "vnode_index_deletes" => 0, "vnode_index_deletes_postings" => 0, "vnode_index_deletes_postings_total" => 0, "vnode_index_deletes_total" => 0,
        "vnode_index_reads" => 0, "vnode_index_reads_total" => 0,
        "vnode_index_refreshes" => 0, "vnode_index_refreshes_total" => 0,
        "vnode_index_writes" => 0, "vnode_index_writes_postings" => 0, "vnode_index_writes_postings_total" => 0, "vnode_index_writes_total" => 0,
        "vnode_puts" => 0, "vnode_puts_total" => 0,
      }

      # The built-in javascript functions available to map/reduce functions
      MAPRED_BUILTINS = File.read(File.dirname(__FILE__) + '/../../../assets/js/mapred_builtins.js')

      attr_accessor :client
      attr_accessor :node

      def initialize(client, node)
        @client = client
        @node = node

        # Initialize the data on the given host
        @data = self.class.server_data[node.host] || begin
          data = {:buckets => {}, :search_indexes => {}, :search_schemas => {}}
          client.nodes.each do |node|
            self.class.server_data[node.host] = data
          end
          data
        end

        set_client_id(0)
      end

      # Verifies the servers is available
      def ping
        true
      end

      # The id reported by this client
      def get_client_id
        @client_id
      end

      # Changes the id reported by this client
      def set_client_id(id)
        @client_id = id.is_a?(Integer) ? [id].pack("N") : id.to_s
      end

      # Information about the node this backend is connected to
      def server_info
        {:node => "riak@#{node.host}", :server_version => '2.0'}
      end

      # Stats reported by Riak.  These are essentially stubbed out with the
      # initial values.
      def stats
        DEFAULT_STATS.dup
      end

      # Gets the data stored in the given bucket / key.  This will raise an
      # exception if the key does not exist
      def fetch_object(bucket, key, options = {})
        result = data(bucket)[:keys][key]
        raise ProtobuffsFailedRequest.new(:not_found, t('not_found')) unless result
        
        load_object(RObject.new(bucket, key), result)
      end

      # Reloads the data in the given object.
      def reload_object(robject, options = {})
        result = data(bucket)[:keys][key]
        raise ProtobuffsFailedRequest.new(:not_found, t('not_found')) unless result

        load_object(robject, result)
      end

      # Updates the data represented by the given object
      def store_object(robject, options = {})
        raw_data = begin; robject.raw_data.dup; rescue TypeError; robject.raw_data; end

        data(robject.bucket)[:keys][robject.key] = {
          :value => raw_data,
          :content_type => robject.content_type.dup,
          :links => robject.links.dup,
          :indexes => robject.indexes.dup,
          :meta => robject.meta.dup,
          :etag => Digest::MD5.hexdigest(raw_data.to_s),
          :last_modified => Time.now.gmtime,
          :vclock => Base64.encode64(Time.now.to_f.to_s).chomp
        }
      end

      # Removes the given key from the server
      def delete_object(bucket, key, options = {})
        data(bucket)[:keys].delete(key)
        true
      end

      # Looks up the numeric value at the given key.  If it's not defined,
      # then this will return 0.
      def get_counter(bucket, key, options = {})
        result = data(bucket)[:keys][key]
        result ? result[:value] : 0
      end

      # Changes the value stored at the key by the given amount.  This amount
      # can be negative or positive.
      def post_counter(bucket, key, amount, options = {})
        value = get_counter(bucket, key, options)

        robject = RObject.new(bucket, key)
        robject.content_type = 'application/riak_pncounter'
        robject.raw_data = value + amount

        store_object(robject, options)
        nil
      end

      # Gets the properties stored in the given bucket.  See Riak::Bucket#props.
      def get_bucket_props(bucket, options = {})
        data(bucket)[:props]
      end

      # Updates the given bucket's properties.  See Riak::Bucket#props for the
      # list of properties available.
      def set_bucket_props(bucket, props, options = {})
        bucket_props = data(bucket)[:props]
        bucket_props.merge!(props)
        bucket_props
      end

      # Resets the given bucket's properties back to its factory defaults.
      def clear_bucket_props(bucket)
        data(bucket)[:props] = DEFAULT_BUCKET_PROPS.dup
      end
      alias_method :reset_bucket_props, :clear_bucket_props

      # Lists all of the keys stored in the given bucket.  If a block is given,
      # the keys will be passed to that block.
      def list_keys(bucket, options = {})
        keys = data(bucket)[:keys].keys

        if block_given?
          yield keys unless keys.empty?
          true
        else
          keys
        end
      end

      # Lists all of the buckets stored on the server.  If a block is given,
      # the buckets will be pased to that block.
      def list_buckets(options = {})
        buckets = @data[:buckets].keys
        buckets.select! {|bucket| list_keys(bucket).any?}

        if block_given?
          yield buckets unless buckets.empty?
          true
        else
          buckets
        end
      end

      # Run a map-reduce process in the Riak cluster
      def mapred(mr, &block)
        results = []
        inputs = []

        # Generate the list of objects to act as inputs into the map/reduce functions
        if mr.inputs.is_a?(String)
          bucket = mr.inputs
          list_keys(bucket).each do |key|
            inputs << [bucket, key]
          end
        else
          inputs.concat(mr.inputs)
        end

        # Run the map/reduce functions
        mr.query.each_with_index do |phase, index|
          case phase.type
          when :map
            result = []
            inputs.each do |input|
              object = mapred_object(*input)
              result.concat(run_mapred_phase(phase, object))
            end
          when :reduce
            result = run_mapred_phase(phase, inputs)
          when :link
            raise NotImplementedError, 'Link walking is deprecated and will not be supported'
          end

          # Track results, use last result as input to the next function
          if phase.keep
            results << result
          elsif index == mr.query.length - 1
            results << []
          end
          
          inputs = result
        end

        results.length == 1 ? results[0] : results
      end

      # Gets a list of objects in the given bucket that are indexed by
      # one or more terms.
      # TODO: support options[:max_results], options[:continuation]
      def get_index(bucket, index, query, options = {}, &block)
        result = IndexCollection.new

        list_keys(bucket).each do |key|
          object = fetch_object(bucket, key)
          object.indexes[index].each do |value|
            # Determine if the object is indexed by one of the values in the query
            found_match =
              if query.is_a?(Range)
                value = value.to_s if query.first.is_a?(String)
                query.include?(value)
              else
                value = value.to_s if query.is_a?(String)
                value == query
              end

            if found_match
              # Track the match
              result << key unless result.include?(key)
              if options[:return_terms]
                result.with_terms ||= {}
                result.with_terms[value] ||= []
                result.with_terms[value] << key
              end
            end
          end
        end

        result
      end

      # Creates a Solr index with the given name
      def create_search_index(name, schema = nil, n_val = nil)
        @data[:search_indexes][name] = {:name => name, :schema => schema || '_yz_default', :n_val => n_val || 3}
        true
      end

      # Gets the schema / n_val associated with the given Solr index
      def get_search_index(name)
        search_index = @data[:search_indexes][name]
        raise ProtobuffsErrorResponse.new(BeefcakeProtobuffsBackend::RpbErrorResp.new(:errcode => 0, :errmsg => 'notfound')) unless search_index

        BeefcakeProtobuffsBackend::RpbYokozunaIndexGetResp.new(
          :index => [BeefcakeProtobuffsBackend::RpbYokozunaIndex.new(search_index)]
        )
      end

      # Updates the schema for the given Solr index
      def update_search_index(name, updates)
        @data[:search_indexes][name][:schema] = updates
      end

      # Deletes the given Solr index from Riak
      def delete_search_index(name)
        @data[:search_indexes].delete(name)
        true
      end

      # Creates a schema for describing how to index fields in Solr
      def create_search_schema(name, content)
        @data[:search_schemas][name] = {:name => name, :content => content}
        true
      end

      # Gets the Solr schema with the given name
      def get_search_schema(name)
        search_schema = @data[:search_schemas][name]
        raise ProtobuffsErrorResponse.new(BeefcakeProtobuffsBackend::RpbErrorResp.new(:errcode => 0, :errmsg => 'notfound')) unless search_schema

        BeefcakeProtobuffsBackend::RpbYokozunaSchema.new(search_schema)
      end

      # Cleans up anything left behind by backend connections
      def teardown
        # No-op
      end

      # = Unimplemented features

      # Runs a Solr search on the given index
      def search(index, query, options = {})
        raise NotImplementedError, 'Search has not been implemented'
      end

      # = Deprecated features

      def link_walk(robject, walk_specs)
        raise NotImplementedError, 'Link walking is deprecated and will not be supported'
      end

      def get_file(filename)
        raise NotImplementedError, 'Luwak is deprecated and will not be supported'
      end

      def file_exists?(filename)
        raise NotImplementedError, 'Luwak is deprecated and will not be supported'
      end

      def delete_file(filename)
        raise NotImplementedError, 'Luwak is deprecated and will not be supported'
      end

      def store_file(*args)
        raise NotImplementedError, 'Luwak is deprecated and will not be supported'
      end

      private
      # Gets the version of the Riak server being reported
      def get_server_version
        server_info[:server_version]
      end

      # Grabs the data currently be stored in the given bucket
      def data(bucket)
        bucket = bucket.name if Bucket === bucket
        @data[:buckets][bucket] ||= {:props => DEFAULT_BUCKET_PROPS.dup, :keys => {}}
      end

      # Loads the given stored data into an RObject
      def load_object(robject, data)
        robject.raw_data = data[:value]
        robject.content_type = data[:content_type]
        robject.links = data[:links]
        robject.indexes = data[:indexes]
        robject.meta = data[:meta]
        robject.etag = data[:etag]
        robject.last_modified = data[:last_modified]
        robject.vclock = data[:vclock]
        robject
      end

      # Generates an object representing the given bucket / key for use within
      # a map-reduce phase
      def mapred_object(bucket = nil, key = nil, data = nil, content_type = nil)
        robject = fetch_object(bucket, key)

        {
          'bucket_type' => 'undefined',
          'bucket' => bucket,
          'key' => key,
          'vclock' => robject.vclock,
          'values' => [
            {
              'metadata' => {
                'X-Riak-VTag' => robject.etag,
                'content-type' => content_type || robject.content_type,
                'index' => robject.indexes,
                'X-Riak-Last-Modified' => robject.last_modified.rfc822,
                'charset' => robject.raw_data.encoding.name
              },
              'data' => data || robject.raw_data
            }
          ]
        }
      end

      # Runs a map-reduce phase for the given input.  This currently only
      # supports javascript phases.
      def run_mapred_phase(phase, input)
        args = [input] + (phase.arg || [])

        if phase.language == 'javascript'
          args = args.map {|arg| arg.to_json} * ','
          ExecJS.eval("(function(){#{MAPRED_BUILTINS};return #{phase.function}(#{args});})()")
        else
          raise NotImplementedError, "Map-Reduce functions implemented in #{phase.language} are not supported"
        end
      end
    end

    # Alias to the names that Riak expects to be available when customizing the
    # backend being used
    MemoryHttpBackend = MemoryBackend
    MemoryProtobuffsBackend = MemoryBackend
  end
end