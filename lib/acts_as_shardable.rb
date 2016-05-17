require 'active_record'
require "acts_as_shardable/version"

module ActsAsShardable

  class WrongShardingError < ::ActiveRecord::ActiveRecordError
  end

  mattr_reader :mutex do
    Mutex.new
  end

  def acts_as_shardable(by:, mod:)
    class_attribute :shard_method, :shard_mod, :module_name, :base_table_name

    mutex.synchronize do
      self.shard_method = by
      self.shard_mod = mod
      self.module_name = self.name.deconstantize.safe_constantize || Object
      self.base_table_name = self.name.demodulize.pluralize.underscore
      self.table_name = "#{self.base_table_name}_0000"
      self.validates self.shard_method.to_sym, presence: true

      # Updates the associated record with values matching those of the instance attributes.
      # Returns the number of affected rows.
      define_method :_update_record do |attribute_names = self.attribute_names|

        was, is = changes[self.class.base_class.shard_method]

        if was
          # shard_column changed
          table_was = self.class.base_class.sharding(was).table_name
          table_is = self.class.base_class.sharding(is).table_name
          raise WrongShardingError, "Please move from #{table_was} to #{table_is} manually."
        else
          # shard_column not changing
          if locking_enabled?
            lock_col = self.class.base_class.locking_column
            previous_lock_value = self[lock_col]
            self[lock_col] = previous_lock_value + 1

            attribute_names += [lock_col].compact
            attribute_names.uniq!

            begin
              relation = self.class.base_class.sharding(is).unscoped

              affected_rows = relation.where(
                  self.class.primary_key => id,
                  lock_col => previous_lock_value,
              ).update_all(
                  Hash[attributes_for_update(attribute_names).map do |name|
                    [name, _read_attribute(name)]
                  end]
              )

              unless affected_rows == 1
                raise ActiveRecord::StaleObjectError.new(self, "update")
              end

              affected_rows

                # If something went wrong, revert the version.
            rescue Exception
              send(lock_col + '=', previous_lock_value)
              raise
            end
          else
            attributes_values = arel_attributes_with_values_for_update(attribute_names)
            if attributes_values.empty?
              0
            else
              shard.unscoped._update_record attributes_values, id, id_was
            end
          end
        end
      end

      private :_update_record


      define_method :touch do |*names|
        raise ActiveRecordError, "cannot touch on a new record object" unless persisted?

        attributes = timestamp_attributes_for_update_in_model
        attributes.concat(names)

        unless attributes.empty?
          current_time = current_time_from_proper_timezone
          changes = {}

          attributes.each do |column|
            column = column.to_s
            changes[column] = write_attribute(column, current_time)
          end

          changes[self.class.locking_column] = increment_lock if locking_enabled?

          clear_attribute_changes(changes.keys)
          primary_key = self.class.primary_key
          shard.unscoped.where(primary_key => self[primary_key]).update_all(changes) == 1
        else
          true
        end
      end

      # Creates a record with values matching those of the instance attributes
      # and returns its id.
      define_method :_create_record do |attribute_names = self.attribute_names|
        attributes_values = arel_attributes_with_values_for_create(attribute_names)

        new_id = shard.unscoped.insert attributes_values
        self.id ||= new_id if self.class.base_class.primary_key

        @new_record = false
        id
      end

      private :_create_record

      define_method :shard do
        self.class.base_class.sharding(self[self.class.base_class.shard_method])
      end

      private :shard

      self.class.send :define_method, :sharding do |column|
        i = column.to_i % shard_mod
        klass = "#{base_class.name.demodulize}_%04d" % i
        @@sharding_class ||= {}
        @@sharding_class[klass] ||= mutex.synchronize do
          if module_name.const_defined?(klass)
            module_name.const_get(klass)
          else
            Class.new(base_class) do
              self.table_name = ("#{base_class.base_table_name}_%04d" % i)

              if base_class.respond_to?(:protobuf_message)
                self.protobuf_message base_class.protobuf_message

                # Create a .to_proto method on XXX::ActiveRecord_Relation
                self.const_get('ActiveRecord_Relation').class_exec do
                  def to_proto(*args)
                    msg_class = base_class.name.demodulize.pluralize
                    module_name = base_class.name.deconstantize.constantize
                    module_name::Messages.const_get(msg_class).new(msg_class.underscore => map { |r| r.to_proto(*args) })
                  end
                end
              end

            end.tap { |k| module_name.const_set(klass, k) }
          end
        end
      end

      define_method :real do
        self.class.sharding(self[self.class.base_class.shard_method]).find(id)
      end
    end

  end

end


# Extend ActiveRecord's functionality
ActiveRecord::Base.send :extend, ActsAsShardable
