require 'spec_helper'
require 'riak/client/memory_backend'
require 'riak/version'

def build_client
  if RIAK_CLIENT_VERSION < '2.0.0'
    Riak::Client.new(:protobuffs_backend => :Memory, :http_backend => :Memory)
  else
    Riak::Client.new(:protobuffs_backend => :Memory)
  end
end

RIAK_CLIENT_VERSION = Riak::VERSION
RIAK_SERVER_VERSION = build_client.backend {|backend| backend.server_info[:server_version]}

describe Riak::Client::MemoryBackend do
  let(:client) do
    build_client
  end
  let(:subject) do
    subject = nil
    client.backend {|backend| subject = backend}
    subject
  end
  let(:bucket) do
    client.bucket('fakeriak')
  end
  let(:search_schema) do
    <<-XML
<?xml version=\"1.0\" encoding=\"UTF-8\" ?>
<schema name=\"#{@index}\" version=\"1.5\">
<fields>
 <field name=\"_yz_id\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" multiValued=\"false\" required=\"true\" />
 <field name=\"_yz_ed\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>
 <field name=\"_yz_pn\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>
 <field name=\"_yz_fpn\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>
 <field name=\"_yz_vtag\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>
 <field name=\"_yz_rk\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>
 <field name=\"_yz_rb\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>
 <field name=\"_yz_rt\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>
 <field name=\"_yz_err\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>
</fields>
<uniqueKey>_yz_id</uniqueKey>
<types>
  <fieldType name=\"_yz_str\" class=\"solr.StrField\" sortMissingLast=\"true\" />
</types>
</schema>
XML
  end

  before(:each) do
    @old_verbose, $VERBOSE = $VERBOSE, nil
    [nil, 'counters', 'sets', 'maps'].each do |bucket_type|
      subject.reset_bucket_props(bucket, :type => bucket_type)
      subject.reset_bucket_type_props(bucket_type)

      bucket.keys(:type => bucket_type).each do |key|
        bucket.delete(key, :type => bucket_type)
      end
    end
  end

  after(:each) do
    $VERBOSE = @old_verbose
  end

  describe 'ping' do
    it 'should be true' do
      expect(client.ping).to eq(true)
    end
  end

  describe 'get_client_id' do
    it 'should be nulled out by default' do
      expect(client.client_id).to eq("\x00\x00\x00\x00")
    end
  end
  
  describe 'set_client_id' do
    it 'should update the client id' do
      client.client_id = 'abc123'
      expect(client.client_id).to eq('abc123')
    end
  end
  
  describe 'server_info' do
    it 'should return the node' do
      expect(subject.server_info[:node]).to eq('riak@127.0.0.1')
    end

    it 'should return the server version' do
      expect(subject.server_info[:node]).not_to be_nil
    end
  end
  
  describe 'fetch_object' do
    it 'should raise an error if not existent' do
      expect { client.get_object('fakeriak', 'fakekey') }.to raise_error(Riak::ProtobuffsFailedRequest)
    end

    it 'should get the value if exists' do
      object = bucket.get_or_new('realkey')
      object.raw_data = 'Hello world'
      object.content_type = 'text/html'
      object.store

      object = client.get_object('fakeriak', 'realkey')
      expect(object.raw_data).to eq('Hello world')
    end
  end
  
  describe 'reload_object' do
    it 'should raise an error if not existent' do
      object = Riak::RObject.new(bucket, 'fakekey')
      expect { client.reload_object(object) }.to raise_error(Riak::ProtobuffsFailedRequest)
    end

    it 'should get the value if exists' do
      object = bucket.get_or_new('realkey')
      object.raw_data = 'Hello world'
      object.content_type = 'text/html'
      object.store

      object = Riak::RObject.new(bucket, 'realkey')
      expect(client.reload_object(object)).to eq(object)
    end
  end

  describe 'store_object' do
    it 'build the object' do
      object = bucket.get_or_new('realkey')
      object.raw_data = 'Hello world'
      object.content_type = 'text/html'

      result = object.store
      expect(result).to eq(object)
    end
  end
  
  describe 'delete_object' do
    before(:each) do
      object = bucket.get_or_new('realkey')
      object.raw_data = 'Hello world'
      object.content_type = 'text/html'
      object.store
    end

    it 'should be true if nonexistent' do
      result = client.delete_object('fakeriak', 'fakekey')
      expect(result).to eq(true)
    end

    it 'should be true if exists' do
      result = client.delete_object('fakeriak', 'realkey')
      expect(result).to eq(true)
      expect { client.get_object('fakeriak', 'realkey') }.to raise_error(Riak::ProtobuffsFailedRequest)
    end
  end
  
  describe 'get_counter' do
    before(:each) do
      bucket.allow_mult = true
    end

    it 'should be 0 if nonexistent' do
      counter = bucket.counter('users')
      expect(counter.value).to eq(0)
    end

    it 'should get current value if exists' do
      counter = bucket.counter('users')
      counter.increment

      counter = bucket.counter('users')
      expect(counter.value).to eq(1)
    end
  end
  
  describe 'post_counter' do
    before(:each) do
      bucket.allow_mult = true
    end

    it 'should return nil' do
      counter = bucket.counter('users')
      result = counter.increment

      expect(result).to eq(nil)
    end
  end
  
  describe 'get_bucket_props' do
    it 'should return default props' do
      expect(bucket.props).to have_key("r")
    end
  end
  
  describe 'set_bucket_props' do
    it 'should update props' do
      bucket.allow_mult = true
      expect(bucket.props).to have_key("allow_mult")
    end
  end
  
  describe 'clear_bucket_props' do
    it 'should clear new props' do
      bucket.allow_mult = true
      bucket.clear_props
      expect(bucket.props).not_to have_key("allow_mult")
    end
  end
  
  describe 'list_keys' do
    it 'should be empty by default' do
      expect(bucket.keys).to eq([])
    end

    it 'should include created keys' do
      object = bucket.get_or_new('realkey')
      object.raw_data = 'Hello world'
      object.content_type = 'text/html'
      object.store

      expect(bucket.keys).to eq(['realkey'])
    end
  end
  
  describe 'list_buckets' do
    it 'should build a collection' do
      expect(client.buckets).to be_instance_of(Array)
    end

    it 'should not new buckets' do
      object = bucket.get_or_new('realkey')
      object.raw_data = 'Hello world'
      object.content_type = 'text/html'
      object.store

      bucket = client.buckets.detect {|bucket| bucket.name == 'fakeriak'}
      expect(bucket).not_to eq(nil)
    end
  end
  
  describe 'mapred' do
    it 'should process' do
      object = bucket.get_or_new('key1')
      object.raw_data = 'Hello world'
      object.content_type = 'text/html'
      object.store

      object = bucket.get_or_new('key2')
      object.raw_data = 'Hello world 2'
      object.content_type = 'text/html'
      object.store

      script = <<-eos
        function(obj) {
          return [obj.values[0].data];
        }
eos
      mapred = Riak::MapReduce.new(client).add(bucket.name).map(script, :keep => true).run
      expect(mapred).to match_array(['Hello world', 'Hello world 2'])
    end
  end
  
  describe 'get_index' do
    before(:each) do
      object1 = bucket.get_or_new('realkey1')
      object1.raw_data = 'test1'
      object1.indexes['index_int'] << 20
      object1.indexes['index_bin'] << '20'
      object1.store

      object2 = bucket.get_or_new('realkey2')
      object2.raw_data = 'test2'
      object2.indexes['index_int'] << 22
      object2.indexes['index_bin'] << '22'
      object2.store
    end

    context 'single index' do
      it 'returns all matching items' do
        expect(client.get_index(bucket, 'index_int', 20)).to eq(['realkey1'])
      end

      it 'should not include any items when nothing matches' do
        expect(client.get_index(bucket, 'index_int', 18)).to eq([])
      end
    end

    context 'index range' do
      it 'should not include items outside of range' do
        expect(client.get_index(bucket, 'index_int', 20..21)).to eq(['realkey1'])
      end

      it 'should include items within range' do
        expect(client.get_index(bucket, 'index_int', 20..22)).to eq(['realkey1', 'realkey2'])
      end

      it 'should not include any items when nothing matches' do
        expect(client.get_index(bucket, 'index_int', 18..19)).to eq([])
      end
    end

    context 'with returned terms' do
      it 'should track which keys matched which terms' do
        index = client.get_index(bucket, 'index_int', 20..22, :return_terms => true)
        expect(index.with_terms).to eq({20 => ['realkey1'], 22 => ['realkey2']})
      end
    end

    context 'with limit' do
      it 'should only return up to the limit' do
        expect(client.get_index(bucket, 'index_int', 20..22, :max_results => 1)).to eq(['realkey1'])
      end

      it 'should return all keys if limit is large enough' do
        expect(client.get_index(bucket, 'index_int', 20..22, :max_results => 5)).to eq(['realkey1', 'realkey2'])
      end
    end

    context 'with continuation' do
      it 'should include only keys after the continuation' do
        expect(client.get_index(bucket, 'index_int', 20..22, :max_results => 1, :continuation => '1')).to eq(['realkey2'])
      end

      it 'should be empty if there are no more matching keys after the continuation' do
        expect(client.get_index(bucket, 'index_int', 20..22, :max_results => 1, :continuation => '2')).to eq([])
      end
    end
  end

  describe 'teardown' do
    it 'should no-op' do
      result = subject.teardown
      expect(result).to eq(nil)
    end
  end
  
  if RIAK_CLIENT_VERSION < '2.0.0'
    describe 'stats' do
      it 'should return stats' do
        expect(subject.stats).not_to be_nil
      end
    end

    describe 'update_search_index' do
      it 'should replace the schema' do
        result = subject.update_search_index('index1', '_yz_custom')
        expect(result).to eq(true)
      end
    end
  end

  if RIAK_CLIENT_VERSION >= '2.0.0' && RIAK_SERVER_VERSION >= '2.0'
    describe 'get_search_index' do
      it 'should fail when index is nonexistent' do
        expect { client.get_search_index('invalid') }.to raise_error(Riak::ProtobuffsErrorResponse)
      end

      it 'should return index when existent' do
        client.create_search_index('index1')
        search_index = client.get_search_index('index1')

        expect(search_index.name).to eq('index1')
        expect(search_index.schema).to eq('_yz_default')
        expect(search_index.n_val).to eq(3)
      end
    end
    
    describe 'create_search_index' do
      it 'should succeed' do
        result = client.create_search_index('index1')
        expect(result).to eq(true)
      end
    end
    
    describe 'get_search_index' do
      it 'should fail when index is nonexistent' do
        expect { client.get_search_index('fake') }.to raise_error(Riak::ProtobuffsErrorResponse)
      end

      it 'should return index when existent' do
        client.create_search_index('index1')
        expect(client.get_search_index('index1')).to_not eq(nil)
      end
    end
    
    describe 'delete_search_index' do
      it 'should remove the index' do
        client.create_search_index('index1', '_yz_default')
        client.delete_search_index('index1')
        expect { client.get_search_index('index1') }.to raise_error(Riak::ProtobuffsErrorResponse)
      end
    end

    describe 'get_search_schema' do
      it 'should fail when schema is nonexistent' do
        expect { client.get_search_schema('fake') }.to raise_error(Riak::ProtobuffsErrorResponse)
      end

      it 'should return schema when existent' do
        client.create_search_schema('schema1', search_schema)
        expect(client.get_search_schema('schema1')).to_not eq(nil)
      end
    end
    
    describe 'create_search_schema' do
      it 'should succeed' do
        result = client.create_search_schema('schema1', search_schema)
        expect(result).to eq(true)
      end
    end

    describe 'get_bucket_type_props' do
      it 'should be empty by default' do
        expect(subject.get_bucket_type_props('foo')).to eq({})
      end

      it 'should get previously set props' do
        subject.set_bucket_type_props('sets', 'datatype' => 'counter')
        expect(subject.get_bucket_type_props('sets')).to eq({'datatype' => 'counter'})
      end
    end

    describe 'set_bucket_type_props' do
      it 'should return the persisted props' do
        result = subject.set_bucket_type_props('sets', 'datatype' => 'set')
        expect(result).to eq('datatype' => 'set')
      end

      it 'should convert symbols to strings' do
        result = subject.set_bucket_type_props('sets', :datatype => 'set')
        expect(result).to eq('datatype' => 'set')
      end
    end

    describe 'crdt_loader' do
      it 'should build a loader instance' do
        expect(subject.crdt_loader).to be_instance_of(Riak::Client::MemoryBackend::CrdtLoader)
      end
    end

    describe 'crdt_operator' do
      it 'should build an operator instance' do
        expect(subject.crdt_operator).to be_instance_of(Riak::Client::MemoryBackend::CrdtOperator)
      end
    end

    describe 'bucket types' do
      let!(:object) do
        object = bucket.get_or_new('realkey')
        object.raw_data = 'Hello world'
        object.content_type = 'text/html'
        object.store(:type => 'counters')
      end

      it 'should scope fetch_object' do
        expect(client.get_object('fakeriak', 'realkey', :type => 'counters')).not_to be_nil
        expect { client.get_object('fakeriak', 'realkey', :type => 'sets') }.to raise_error(Riak::ProtobuffsFailedRequest)
      end

      it 'should scope reload_object' do
        object = Riak::RObject.new(bucket, 'realkey')
        expect(client.reload_object(object, :type => 'counters')).to eq(object)
        expect { client.reload_object(object, :type => 'sets') }.to raise_error(Riak::ProtobuffsFailedRequest)
      end

      it 'should scope store_object' do
        object = bucket.get_or_new('realkey')
        object.raw_data = 'Hello world other'
        object.content_type = 'text/html'
        object.store(:type => 'sets')

        expect(client.get_object('fakeriak', 'realkey', :type => 'counters').raw_data).to eq('Hello world')
        expect(client.get_object('fakeriak', 'realkey', :type => 'sets').raw_data).to eq('Hello world other')
      end

      it 'should scope delete_object' do
        client.delete_object('fakeriak', 'realkey', :type => 'sets')
        expect { client.get_object('fakeriak', 'realkey', :type => 'counters') }.not_to raise_error
        expect(client.delete_object('fakeriak', 'realkey', :type => 'counters')).to eq(true)
        expect { client.get_object('fakeriak', 'realkey', :type => 'counters') }.to raise_error
      end

      it 'should scope get_counter' do
        bucket.allow_mult = true
        bucket.counter('users').increment(1, :type => 'counters')
        expect(bucket.counter('users').value(:type => 'sets')).to eq(0)
        expect(bucket.counter('users').value(:type => 'counters')).to eq(1)
      end

      it 'should scope get_bucket_props' do
        client.set_bucket_props(bucket, {:allow_mult => true}, 'counters')
        expect(client.get_bucket_props(bucket, :type => 'sets')['allow_mult']).to be_nil
        expect(client.get_bucket_props(bucket, :type => 'counters')['allow_mult']).to eq(true)
      end

      it 'should scope set_bucket_props' do
        client.set_bucket_props(bucket, {:allow_mult => false}, 'counters')
        client.set_bucket_props(bucket, {:allow_mult => true}, 'sets')
        expect(client.get_bucket_props(bucket, :type => 'counters')['allow_mult']).to eq(false)
        expect(client.get_bucket_props(bucket, :type => 'sets')['allow_mult']).to eq(true)
      end

      it 'should scope clear_bucket_props' do
        client.set_bucket_props(bucket, {:allow_mult => false}, 'counters')
        client.set_bucket_props(bucket, {:allow_mult => true}, 'sets')
        subject.clear_bucket_props(bucket, :type => 'sets')
        expect(client.get_bucket_props(bucket, :type => 'counters')['allow_mult']).to eq(false)
        expect(client.get_bucket_props(bucket, :type => 'sets')['allow_mult']).to be_nil
      end

      it 'should scope list_keys' do
        expect(client.list_keys(bucket)).to eq([])
        expect(client.list_keys(bucket, :type => 'counters')).to eq(['realkey'])
      end

      it 'should scope list_buckets' do
        expect(client.list_buckets).to eq([])
        expect(client.list_buckets(:type => 'counters')).to eq([bucket])
      end

      it 'should scope get_index' do
        object.indexes['index_int'] << 20
        object.store(:type => 'counters')

        expect(client.get_index(bucket, 'index_int', 20)).to eq([])
        expect(client.get_index(bucket, 'index_int', 20, :type => 'counters')).to eq(['realkey'])
      end
    end

    describe 'crdts' do
      before(:each) do
        subject.set_bucket_type_props('counters', :datatype => 'counter')
        subject.set_bucket_type_props('sets', :datatype => 'set')
        subject.set_bucket_type_props('maps', :datatype => 'map')
      end

      describe 'counter' do
        let(:counter) do
          Riak::Crdt::Counter.new(bucket, 'users')
        end

        it 'should be 0 by default' do
          expect(counter.value).to eq(0)
        end

        it 'should increment' do
          expect(counter.increment).to eq(true)
        end

        it 'should increment with custom value' do
          expect(counter.increment(3)).to eq(true)
        end

        it 'should decrement' do
          expect(counter.decrement).to eq(true)
        end

        it 'should track value' do
          counter.increment
          counter.increment(3)
          counter.decrement(2)

          expect(counter.value).to eq(2)
        end

        it 'should process batches' do
          counter.batch do |c|
            c.increment
            c.increment(3)
            c.decrement(2)
          end

          expect(counter.value).to eq(2)
        end

        it 'should store data scoped to the type' do
          counter.increment
          expect(client.get_object(bucket, 'users', :type => 'counters')).not_to be_nil
        end

        it 'should set the content type on the object' do
          counter.increment
          object = client.get_object(bucket, 'users', :type => 'counters')
          expect(object.content_type).to eq('application/riak_counter')
        end
      end

      describe 'set' do
        let(:set) do
          Riak::Crdt::Set.new(bucket, 'users')
        end

        it 'should be an empty set by default' do
          expect(set.value).to eq(Set.new)
        end

        it 'should add values' do
          expect(set.add('john')).to eq(true)
        end

        it 'should remove values' do
          expect(set.add('smith')).to eq(true)
        end

        it 'should track changes' do
          set.add('john')
          set.add('smith')
          set.reload
          set.remove('smith')

          expect(set.value).to eq(Set.new(['john']))
        end

        it 'should process batches' do
          set.add('joe')
          set.reload
          set.batch do |s|
            s.add('john')
            s.add('smith')
            s.remove('joe')
          end

          expect(set.value).to eq(Set.new(['john', 'smith']))
        end

        it 'should store data scoped to the type' do
          set.add('john')
          expect(client.get_object(bucket, 'users', :type => 'sets')).not_to be_nil
        end

        it 'should set the content type on the object' do
          set.add('john')
          object = client.get_object(bucket, 'users', :type => 'sets')
          expect(object.content_type).to eq('application/riak_set')
        end
      end

      describe 'map' do
        let(:map) do
          Riak::Crdt::Map.new(bucket, 'user')
        end

        if RUBY_VERSION >= '2.1.0'
          it 'should be an empty set by default' do
            expect(map.value).to eq({'counters' => {}, 'flags' => {}, 'maps' => {}, 'registers' => {}, 'sets' => {}})
          end
        end

        it 'should set the content type on the object' do
          map.counters['points'].increment

          object = client.get_object(bucket, 'user', :type => 'maps')
          expect(object.content_type).to eq('application/riak_map')
        end

        describe 'set' do
          it 'should be an empty set by default' do
            expect(map.sets['colors'].value).to eq(Set.new)
          end

          it 'should track changes' do
            map.sets['colors'].add('red')
            map.sets['colors'].add('blue')
            expect(map.sets['colors'].value).to eq(Set.new(['red', 'blue']))
          end

          it 'should allow deletion' do
            map.sets['colors'].add('red')
            map.sets.delete('colors')
            expect(map.sets['colors'].value).to eq(Set.new)
          end
        end

        describe 'counter' do
          it 'should be 0 by default' do
            expect(map.counters['points'].value).to eq(0)
          end

          it 'should track changes' do
            map.counters['points'].increment
            map.counters['points'].increment
            expect(map.counters['points'].value).to eq(2)
          end

          it 'should allow deletion' do
            map.counters['points'].increment
            map.counters.delete('points')
            expect(map.counters['points'].value).to eq(0)
          end
        end
        
        describe 'flag' do
          if RIAK_CLIENT_VERSION > '2.1.0'
            it 'should be falsey by default' do
              expect(map.flags['enabled']).to be_falsey
            end
          end

          it 'should track changes' do
            map.flags['enabled'] = true
            expect(map.flags['enabled']).to eq(true)

            map.flags['enabled'] = false
            expect(map.flags['enabled']).to eq(false)
          end

          if RIAK_CLIENT_VERSION > '2.1.0'
            it 'should allow deletion' do
              map.flags['enabled'] = true
              map.flags.delete('enabled')
              expect(map.flags['enabled']).to be_falsey
            end
          end
        end
        
        describe 'register' do
          it 'should be nil by default' do
            expect(map.registers['name']).to be_nil
          end

          it 'should track changes' do
            map.registers['name'] = 'John'
            expect(map.registers['name']).to eq('John')

            map.registers['name'] = 'Johnny'
            expect(map.registers['name']).to eq('Johnny')
          end

          it 'should allow deletion' do
            map.registers['name'] = 'John'
            map.registers.delete('name')
            expect(map.registers['name']).to be_nil
          end
        end
        
        describe 'sub-map' do
          if RUBY_VERSION >= '2.1.0'
            it 'should be empty by default' do
              expect(map.maps['parent'].value).to eq({'counters' => {}, 'flags' => {}, 'maps' => {}, 'registers' => {}, 'sets' => {}})
            end
          end

          it 'should track changes' do
            map.maps['parent'].sets['colors'].add('red')
            map.maps['parent'].counters['points'].increment
            map.maps['parent'].flags['enabled'] = true
            map.maps['parent'].registers['name'] = 'John'

            expect(map.maps['parent'].sets['colors'].value).to eq(Set.new(['red']))
            expect(map.maps['parent'].counters['points'].value).to eq(1)
            expect(map.maps['parent'].flags['enabled']).to eq(true)
            expect(map.maps['parent'].registers['name']).to eq('John')
          end

          it 'should not track changes in root' do
            map.maps['parent'].sets['colors'].add('red')

            expect(map.sets['colors'].value).to eq(Set.new)
          end

          it 'should allow deletion' do
            map.maps['parent'].counters['points'].increment
            map.maps.delete('parent')
            expect(map.maps['parent'].counters['points'].value).to eq(0)
          end
        end

        it 'should process batches' do
          map.batch do |m|
            m.counters['points'].increment
            m.sets['colors'].add('red')
          end

          expect(map.counters['points'].value).to eq(1)
          expect(map.sets['colors'].value).to eq(Set.new(['red']))
        end
      end
    end
  end

  describe 'search' do
    it 'should raise an error' do
      expect { subject.search('test', 'a=1') }.to raise_error(NotImplementedError)
    end
  end
  
  describe 'link_walk' do
    it 'should raise an error' do
      expect { subject.link_walk('test1', 'test2') }.to raise_error(NotImplementedError)
    end
  end
  
  describe 'get_file' do
    it 'should raise an error' do
      expect { subject.get_file('test') }.to raise_error(NotImplementedError)
    end
  end
  
  describe 'file_exists?' do
    it 'should raise an error' do
      expect { subject.file_exists?('test') }.to raise_error(NotImplementedError)
    end
  end
  
  describe 'delete_file' do
    it 'should raise an error' do
      expect { subject.delete_file('test') }.to raise_error(NotImplementedError)
    end
  end
  
  describe 'store_file' do
    it 'should raise an error' do
      expect { subject.store_file('test') }.to raise_error(NotImplementedError)
    end
  end
end
