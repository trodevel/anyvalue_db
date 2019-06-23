/*

Table.

Copyright (C) 2019 Sergey Kolevatov

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.

*/

// $Revision: 11816 $ $Date:: 2019-06-23 #$ $Author: serge $

#ifndef ANYVALUE_DB__TABLE_H
#define ANYVALUE_DB__TABLE_H

#include "anyvalue/op_less.h"       // operator<

namespace std
{
inline bool operator<( anyvalue::Value const & lhs, anyvalue::Value const & rhs )
{
    return ::operator<( lhs, rhs );
}
}

#include <mutex>            // std::mutex
#include <map>              // std::map
#include <set>              // std::set

#include "record.h"         // Record
#include "status.h"         // Status

#include "i_table.h"        // ITable

#include "anyvalue/operations.h"    // anyvalue::comparison_type_e

namespace anyvalue_db
{

class Table: public ITable
{
    friend class Serializer;
    friend class StrHelper;

public:

    Table();
    ~Table();

    bool init(
            const std::string & filename );

    bool init(
            const std::vector<field_id_t> & keys );

    bool add_record(
            Record              * record,
            std::string         * error_msg );

    Record* create_record__unlocked(
            std::string         * error_msg );

    bool delete_record__unlocked(
            Record              * record,
            std::string         * error_msg );

    bool delete_record__unlocked( field_id_t field_id, const Value & value );

    bool on_add_field( field_id_t field_id, const Value & value, Record * record ) override;
    bool on_update_field( field_id_t field_id, const Value & old_value, const Value & new_value, Record * record ) override;
    void on_delete_field( field_id_t field_id, const Value & value ) override;

    Record* find__unlocked( field_id_t field_id, const Value & value );
    const Record* find__unlocked( field_id_t field_id, const Value & value ) const;

    std::vector<Record*> select__unlocked( field_id_t field_id, anyvalue::comparison_type_e op, const Value & value ) const;

    bool save( std::string * error_msg, const std::string & filename ) const;

    std::mutex & get_mutex() const;

private:

    typedef std::set<Record*>           SetRecord;
    typedef std::map<Value,Record*>     MapValueIdToRecord;
    typedef std::map<field_id_t,MapValueIdToRecord>     MapFieldIdToIndex;

private:

    bool save_intern( std::string * error_msg, const std::string & filename ) const;
    bool load_intern( const std::string & filename );

    void get_status( Status * res ) const;
    bool init_index(
            const std::vector<field_id_t> & keys );
    bool init_from_status( std::string * error_msg, const Status & status );

    bool add_record__unlocked(
            Record              * record,
            std::string         * error_msg );

    void cleanup_index_for_record( Record * record );
    void cleanup_index_for_record_field( Record * record, field_id_t field_id, MapValueIdToRecord & map );

    bool validate_keys_of_new_record( const Record & record, std::string * error_msg ) const;
    bool validate_keys_of_new_record_field( const Record & record, field_id_t field_id, const MapValueIdToRecord & map, std::string * error_msg ) const;

    void add_index_for_record( Record * record );
    void add_index_for_record_field( Record * record, field_id_t field_id, MapValueIdToRecord & map );

private:
    mutable std::mutex          mutex_;

    bool                        is_inited_;

    // Config
    std::string                 credentials_file_;

    SetRecord                   records_;
    MapFieldIdToIndex           map_field_id_to_index_;
};

} // namespace anyvalue_db


#endif // ANYVALUE_DB__TABLE_H
