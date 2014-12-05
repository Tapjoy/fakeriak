require 'riak'
require 'riak/util/translation'

begin
  # Riak 2.x
  require 'riak/errors/failed_request'
rescue LoadError
  # Riak 1.x
  require 'riak/failed_request'
end

module Riak
  class Client
    class MemoryBackend
      class << self
        attr_reader :server_data

        def configured?
          true
        end
      end

      include Util::Translation

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

      attr_accessor :client
      attr_accessor :node
      def initialize(client, node)
        @client = client
        @node = node
        @data = self.class.server_data[node.host] || begin
          data = {:buckets => {}, :search_indexes => {}, :search_schemas => {}}
          client.nodes.each do |node|
            self.class.server_data[node.host] = data
          end
          data
        end

        set_client_id(0)
      end

      def ping
        true
      end

      def get_client_id
        @client_id
      end

      def set_client_id(id)
        @client_id = id.is_a?(Integer) ? [id].pack("N") : id.to_s
      end

      def server_info
        {:node => "riak@#{node.host}", :server_version => '2.0'}
      end

      def stats
        {
          "basho_stats_version" => "1.0.3",
          "bitcask_version" => "1.6.6-0-g230b6d6",
          "cluster_info_version" => "1.2.4",
          "compiler_version" => "4.8.1",
          "connected_nodes" => [],
          "converge_delay_last" => 0,
          "converge_delay_max" => 0,
          "converge_delay_mean" => 0,
          "converge_delay_min" => 0,
          "coord_redirs_total" => 0,
          "cpu_avg1" => 0,
          "cpu_avg15" => 0,
          "cpu_avg5" => 0,
          "cpu_nprocs" => 0,
          "crypto_version" => "2.1",
          "dropped_vnode_requests_total" => 0,
          "erlang_js_version" => "1.2.2",
          "erlydtl_version" => "0.7.0",
          "executing_mappers" => 0,
          "goldrush_version" => "0.1.5",
          "gossip_received" => 0,
          "handoff_timeouts" => 0,
          "ignored_gossip_total" => 0,
          "index_fsm_active" => 0,
          "index_fsm_create" => 0,
          "index_fsm_create_error" => 0,
          "inets_version" => "5.9",
          "kernel_version" => "2.15.1",
          "lager_version" => "2.0.1",
          "leveldb_read_block_error" => "undefined",
          "list_fsm_active" => 0,
          "list_fsm_create" => 0,
          "list_fsm_create_error" => 0,
          "mem_allocated" => 0,
          "mem_total" => 0,
          "memory_atom" => 0,
          "memory_atom_used" => 0,
          "memory_binary" => 0,
          "memory_code" => 0,
          "memory_ets" => 0,
          "memory_processes" => 0,
          "memory_processes_used" => 0,
          "memory_system" => 0,
          "memory_total" => 0,
          "merge_index_version" => "1.3.2-0-gcb38ee7",
          "mochiweb_version" => "1.5.1p6",
          "node_get_fsm_active" => 0,
          "node_get_fsm_active_60s" => 0,
          "node_get_fsm_in_rate" => 0,
          "node_get_fsm_objsize_100" => 0,
          "node_get_fsm_objsize_95" => 0,
          "node_get_fsm_objsize_99" => 0,
          "node_get_fsm_objsize_mean" => 0,
          "node_get_fsm_objsize_median" => 0,
          "node_get_fsm_out_rate" => 0,
          "node_get_fsm_rejected" => 0,
          "node_get_fsm_rejected_60s" => 0,
          "node_get_fsm_rejected_total" => 0,
          "node_get_fsm_siblings_100" => 0,
          "node_get_fsm_siblings_95" => 0,
          "node_get_fsm_siblings_99" => 0,
          "node_get_fsm_siblings_mean" => 0,
          "node_get_fsm_siblings_median" => 0,
          "node_get_fsm_time_100" => 0,
          "node_get_fsm_time_95" => 0,
          "node_get_fsm_time_99" => 0,
          "node_get_fsm_time_mean" => 0,
          "node_get_fsm_time_median" => 0,
          "node_gets" => 0,
          "node_gets_total" => 0,
          "node_put_fsm_active" => 0,
          "node_put_fsm_active_60s" => 0,
          "node_put_fsm_in_rate" => 0,
          "node_put_fsm_out_rate" => 0,
          "node_put_fsm_rejected" => 0,
          "node_put_fsm_rejected_60s" => 0,
          "node_put_fsm_rejected_total" => 0,
          "node_put_fsm_time_100" => 0,
          "node_put_fsm_time_95" => 0,
          "node_put_fsm_time_99" => 0,
          "node_put_fsm_time_mean" => 0,
          "node_put_fsm_time_median" => 0,
          "node_puts" => 0,
          "node_puts_total" => 0,
          "nodename" => "riak@127.0.0.1",
          "os_mon_version" => "2.2.9",
          "pbc_active" => 1,
          "pbc_connects" => 0,
          "pbc_connects_total" => 0,
          "pipeline_active" => 0,
          "pipeline_create_count" => 0,
          "pipeline_create_error_count" => 0,
          "pipeline_create_error_one" => 0,
          "pipeline_create_one" => 0,
          "postcommit_fail" => 0,
          "precommit_fail" => 0,
          "public_key_version" => "0.15",
          "read_repairs" => 0,
          "read_repairs_total" => 0,
          "rebalance_delay_last" => 0,
          "rebalance_delay_max" => 0,
          "rebalance_delay_mean" => 0,
          "rebalance_delay_min" => 0,
          "rejected_handoffs" => 0,
          "riak_api_version" => "1.4.10-0-gc407ac0",
          "riak_control_version" => "1.4.10-0-g73c43c3",
          "riak_core_stat_ts" => 1416840306,
          "riak_core_version" => "1.4.10",
          "riak_kv_stat_ts" => 1416840306,
          "riak_kv_version" => "1.4.10-0-g64b6ad8",
          "riak_kv_vnodeq_max" => 0,
          "riak_kv_vnodeq_mean" => 0,
          "riak_kv_vnodeq_median" => 0,
          "riak_kv_vnodeq_min" => 0,
          "riak_kv_vnodeq_total" => 0,
          "riak_kv_vnodes_running" => 64,
          "riak_pipe_stat_ts" => 1416840305,
          "riak_pipe_version" => "1.4.10-0-g9353526",
          "riak_pipe_vnodeq_max" => 0,
          "riak_pipe_vnodeq_mean" => 0,
          "riak_pipe_vnodeq_median" => 0,
          "riak_pipe_vnodeq_min" => 0,
          "riak_pipe_vnodeq_total" => 0,
          "riak_pipe_vnodes_running" => 64,
          "riak_search_version" => "1.4.10-0-g6e548e7",
          "riak_sysmon_version" => "1.1.3",
          "ring_creation_size" => 64,
          "ring_members" => ["riak@127.0.0.1"],
          "ring_num_partitions" => 64,
          "ring_ownership" => "[{'riak@127.0.0.1',64}]",
          "rings_reconciled" => 0,
          "rings_reconciled_total" => 0,
          "runtime_tools_version" => "1.8.8",
          "sasl_version" => "2.2.1",
          "sidejob_version" => "0.2.0",
          "ssl_version" => "5.0.1",
          "stdlib_version" => "1.18.1",
          "storage_backend" => "riak_kv_bitcask_backend",
          "syntax_tools_version" => "1.6.8",
          "sys_driver_version" => "2.0",
          "sys_global_heaps_size" => 0,
          "sys_heap_type" => "private",
          "sys_logical_processors" => 8,
          "sys_otp_release" => "R15B01",
          "sys_process_count" => 1900,
          "sys_smp_support" => true,
          "sys_system_architecture" => "x86_64-unknown-linux-gnu",
          "sys_system_version" => 0,
          "sys_thread_pool_size" => 64,
          "sys_threads_enabled" => true,
          "sys_wordsize" => 8,
          "vnode_gets" => 0,
          "vnode_gets_total" => 0,
          "vnode_index_deletes" => 0,
          "vnode_index_deletes_postings" => 0,
          "vnode_index_deletes_postings_total" => 0,
          "vnode_index_deletes_total" => 0,
          "vnode_index_reads" => 0,
          "vnode_index_reads_total" => 0,
          "vnode_index_refreshes" => 0,
          "vnode_index_refreshes_total" => 0,
          "vnode_index_writes" => 0,
          "vnode_index_writes_postings" => 0,
          "vnode_index_writes_postings_total" => 0,
          "vnode_index_writes_total" => 0,
          "vnode_puts" => 0,
          "vnode_puts_total" => 0,
          "webmachine_version" => "1.10.4-0-gfcff795",
        }
      end

      def fetch_object(bucket, key, options = {})
        result = data(bucket)[:keys][key]
        raise ProtobuffsFailedRequest.new(:not_found, t('not_found')) unless result
        result
      end

      def reload_object(robject, options = {})
        robject.raw_data = fetch_object(robject.bucket, robject.key).raw_data
        robject
      end

      def store_object(robject, options = {})
        data(robject.bucket)[:keys][robject.key] = robject
      end

      def delete_object(bucket, key, options = {})
        data(bucket)[:keys].delete(key)
        true
      end

      def link_walk(robject, walk_specs)
        raise NotImplementedError, 'Link walking is deprecated and will not be supported'
      end

      def get_counter(bucket, key, options = {})
        result = data(bucket)[:keys][key]
        result ? result.raw_data : 0
      end

      def post_counter(bucket, key, amount, options = {})
        value = get_counter(bucket, key, options)

        robject = RObject.new(bucket, key)
        robject.content_type = 'application/riak_pncounter'
        robject.raw_data = value + amount

        store_object(robject, options)
        nil
      end

      def get_bucket_props(bucket, options = {})
        data(bucket)[:props]
      end

      def set_bucket_props(bucket, props, options = {})
        bucket_props = data(bucket)[:props]
        bucket_props.merge!(props)
        bucket_props
      end

      def reset_bucket_props(bucket)
        clear_bucket_props(bucket)
      end

      def clear_bucket_props(bucket)
        data(bucket)[:props] = DEFAULT_BUCKET_PROPS.dup
      end

      def list_keys(bucket, options = {})
        keys = data(bucket)[:keys].keys

        if block_given?
          yield keys unless keys.empty?
          true
        else
          keys
        end
      end

      def list_buckets(options = {})
        buckets = @data[:buckets].keys

        if block_given?
          yield buckets unless buckets.empty?
          true
        else
          buckets
        end
      end

      def mapred(mr, &block)
        raise NotImplementedError
      end

      def get_index(bucket, index, query, options = {}, &block)
        result = IndexCollection.new

        list_keys(bucket).each do |key|
          object = fetch_object(bucket, key)
          object.indexes[index].each do |value|
            found_match =
              if query.is_a?(Range)
                value = value.to_s if query.first.is_a?(String)
                query.include?(value)
              else
                value = value.to_s if query.is_a?(String)
                value == query
              end

            if found_match
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

      def search(index, query, options = {})
        raise NotImplementedError
      end

      def create_search_index(name, schema = nil, n_val = nil)
        @data[:search_indexes][name] = {:name => name, :schema => schema || '_yz_default', :n_val => n_val || 3}
        true
      end

      def get_search_index(name)
        search_index = @data[:search_indexes][name]
        raise ProtobuffsErrorResponse.new(BeefcakeProtobuffsBackend::RpbErrorResp.new(:errcode => 0, :errmsg => 'notfound')) unless search_index

        BeefcakeProtobuffsBackend::RpbYokozunaIndexGetResp.new(
          :index => [BeefcakeProtobuffsBackend::RpbYokozunaIndex.new(search_index)]
        )
      end

      def update_search_index(name, updates)
        @data[:search_indexes][name][:schema] = updates
      end

      def delete_search_index(name)
        @data[:search_indexes].delete(name)
        true
      end

      def create_search_schema(name, content)
        @data[:search_schemas][name] = {:name => name, :content => content}
        true
      end

      def get_search_schema(name)
        search_schema = @data[:search_schemas][name]
        raise ProtobuffsErrorResponse.new(BeefcakeProtobuffsBackend::RpbErrorResp.new(:errcode => 0, :errmsg => 'notfound')) unless search_schema

        BeefcakeProtobuffsBackend::RpbYokozunaSchema.new(search_schema)
      end

      def teardown
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
      def get_server_version
        server_info[:server_version]
      end

      def data(bucket)
        bucket = bucket.name if Bucket === bucket
        @data[:buckets][bucket] ||= {:props => DEFAULT_BUCKET_PROPS.dup, :keys => {}}
      end
    end

    MemoryHttpBackend = MemoryBackend
    MemoryProtobuffsBackend = MemoryBackend
  end
end