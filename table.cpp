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

// $Revision: 11943 $ $Date:: 2019-09-09 #$ $Author: serge $

#include "table.h"                      // self

#include <fstream>                      // std::ifstream

#include "utils/mutex_helper.h"         // MUTEX_SCOPE_LOCK
#include "utils/dummy_logger.h"         // dummy_log
#include "utils/utils_assert.h"         // ASSERT
#include "utils/rename_and_backup.h"    // utils::rename_and_backup
#include "anyvalue/value_operations.h"  // anyvalue::compare_values
#include "anyvalue/str_helper.h"        // anyvalue::StrHelper

#include "str_helper.h"                 // StrHelper
#include "serializer.h"                 // serializer::load

#define MODULENAME      "Table"

namespace anyvalue_db
{

Table::Table():
        is_inited_( false )
{
}

Table::~Table()
{
    for( auto e: records_ )
    {
        delete e;
    }
}

bool Table::init(
        const std::string   & filename )
{
    MUTEX_SCOPE_LOCK( mutex_ );

    assert( is_inited_ == false );

    auto b = load_intern( filename );

    if( b )
        is_inited_  = true;

    return b;
}

bool Table::init(
        const std::vector<field_id_t> & keys )
{
    MUTEX_SCOPE_LOCK( mutex_ );

    assert( is_inited_ == false );

    auto b = init_index( keys );

    if( b )
        is_inited_  = true;

    return b;
}

bool Table::add_record(
        Record              * record,
        std::string         * error_msg )
{
    MUTEX_SCOPE_LOCK( mutex_ );

    return add_record__unlocked( record, error_msg );
}

bool Table::add_record__unlocked(
        Record              * record,
        std::string         * error_msg )
{
    return add_record__unlocked__intern( record, error_msg, false );
}

bool Table::add_loaded_record__unlocked(
        Record              * record,
        std::string         * error_msg )
{
    return add_record__unlocked__intern( record, error_msg, true );
}

bool Table::add_record__unlocked__intern(
        Record              * record,
        std::string         * error_msg,
        bool                is_loaded )
{
    assert( ( is_inited_ && is_loaded == false ) || ( is_inited_ == false && is_loaded ) );

    std::string error_msg_2;

    if( validate_keys_of_new_record( * record, & error_msg_2 ) == false )
    {
        dummy_log_error( MODULENAME, "add_record__unlocked: key validation failure %s", error_msg_2.c_str() );

        * error_msg = "key validation failure " + error_msg_2;

        return false;
    }

    auto b = records_.insert( record ).second;

    if( b == false )
    {
        dummy_log_error( MODULENAME, "add_record__unlocked: record %p already exists", record );

        * error_msg = "record already exists";

        return false;
    }

    add_index_for_record( record );

    record->set_parent( this );

    return true;
}

Record* Table::create_record__unlocked(
        std::string         * error_msg )
{
    assert( is_inited_ );

    auto res = new Record( this );

    auto b = records_.insert( res ).second;

    assert( b );    // should never happen

    dummy_log_info( MODULENAME, "create_record__unlocked: created new record %p", res );

    return res;
}

bool Table::delete_record__unlocked(
        Record              * record,
        std::string         * error_msg )
{
    assert( is_inited_ );

    auto it = records_.find( record );
    if( it == records_.end() )
    {
        * error_msg   = "record " + std::to_string( reinterpret_cast<std::uintptr_t>( record ) ) + " not found";
        dummy_log_error( MODULENAME, "delete_record__unlocked: record %p not found", record );
        return false;
    }

    records_.erase( it );

    cleanup_index_for_record( record );

    delete record;

    dummy_log_info( MODULENAME, "delete_record__unlocked: record %p deleted", record );

    return true;
}

bool Table::delete_record__unlocked(
        field_id_t          field_id,
        const Value         & value,
        std::string         * error_msg )
{
    assert( is_inited_ );

    auto rec = find__unlocked( field_id, value );

    if( rec == nullptr )
    {
        dummy_log_debug( MODULENAME, "delete_record__unlocked: field id %u w/ value %s not found", field_id, anyvalue::StrHelper::to_string( value ).c_str() );

        * error_msg = "field id " + std::to_string( field_id ) + " w/ value " + anyvalue::StrHelper::to_string( value ) + " not found";

        return false;
    }

    auto b = delete_record__unlocked( rec, error_msg );

    if( b )
    {
        dummy_log_info( MODULENAME, "delete_record__unlocked: delete - field id %u w/ value %s", field_id, anyvalue::StrHelper::to_string( value ).c_str() );
    }
    else
    {
        dummy_log_info( MODULENAME, "delete_record__unlocked: cannot delete record - field id %u w/ value %s", field_id, anyvalue::StrHelper::to_string( value ).c_str() );
    }

    return b;
}

bool Table::on_add_field( field_id_t field_id, const Value & value, Record * record )
{
    auto it = map_field_id_to_index_.find( field_id );

    if( it == map_field_id_to_index_.end() )
        return true;

    auto & map = it->second;

    auto it_2 = map.find( value );

    if( it_2 != map.end() )
        return false;       // value already exists, not possible to insert it again as it will destroy index

    auto b = map.insert( std::make_pair( value, record ) ).second;

    assert( b );

    return true;
}

bool Table::on_update_field( field_id_t field_id, const Value & old_value, const Value & new_value, Record * record )
{
    auto it = map_field_id_to_index_.find( field_id );

    if( it == map_field_id_to_index_.end() )
        return true;

    auto & map = it->second;

    auto it_2 = map.find( old_value );

    assert( it_2 != map.end() );    // old value must exist

    auto it_3 = map.find( new_value );

    if( it_3 != map.end() )
        return false;       // new value already exists, not possible to insert it again as it will destroy index

    map.erase( it_2 );

    auto b = map.insert( std::make_pair( new_value, record ) ).second;

    assert( b );

    return true;
}

void Table::on_delete_field( field_id_t field_id, const Value & value )
{
    auto it = map_field_id_to_index_.find( field_id );

    if( it == map_field_id_to_index_.end() )
        return;

    auto & map = it->second;

    auto it_2 = map.find( value );

    assert( it_2 != map.end() );    // value must exist

    map.erase( it_2 );
}


void Table::cleanup_index_for_record( Record * record )
{
    for( auto & e : map_field_id_to_index_ )
    {
        cleanup_index_for_record_field( record, e.first, e.second );
    }
}

void Table::cleanup_index_for_record_field( Record * record, field_id_t field_id, MapValueIdToRecord & map )
{
    Value v;

    if( record->get_field( field_id, & v ) == false )
        return;

    auto it = map.find( v );

    if( it == map.end() )
    {
        dummy_log_error( MODULENAME, "cleanup_index_for_record_field: record %p, field_id %u, cannot find value %s", record, field_id, anyvalue::StrHelper::to_string( v ).c_str() );
        return;
    }

    map.erase( it );

    dummy_log_debug( MODULENAME, "cleanup_index_for_record_field: record %p, field_id %u, value %s - OK", record, field_id, anyvalue::StrHelper::to_string( v ).c_str() );
}

void Table::add_index_for_record( Record * record )
{
    for( auto & e : map_field_id_to_index_ )
    {
        add_index_for_record_field( record, e.first, e.second );
    }
}

void Table::add_index_for_record_field( Record * record, field_id_t field_id, MapValueIdToRecord & map )
{
    Value v;

    if( record->get_field( field_id, & v ) == false )
        return;

    auto b = map.insert( std::make_pair( v, record ) ).second;

    if( b == false )
    {
        dummy_log_error( MODULENAME, "add_index_for_record_field: record %p, field_id %u, duplicate value %s", record, field_id, anyvalue::StrHelper::to_string( v ).c_str() );
        return;
    }

    dummy_log_debug( MODULENAME, "add_index_for_record_field: record %p, field_id %u, value %s - OK", record, field_id, anyvalue::StrHelper::to_string( v ).c_str() );
}

bool Table::validate_keys_of_new_record( const Record & record, std::string * error_msg ) const
{
    for( auto & e : map_field_id_to_index_ )
    {
        if( validate_keys_of_new_record_field( record, e.first, e.second, error_msg ) == false )
            return false;
    }

    return true;
}

bool Table::validate_keys_of_new_record_field( const Record & record, field_id_t field_id, const MapValueIdToRecord & map, std::string * error_msg ) const
{
    Value v;

    if( record.get_field( field_id, & v ) == false )
        return true;

    auto it = map.find( v );

    if( it != map.end() )
    {
        * error_msg = "field id " + std::to_string( field_id ) + ", value " + anyvalue::StrHelper::to_string( v ) + " already exists";

        return false;
    }

    return true;
}

Record* Table::find__unlocked( field_id_t field_id, const Value & value )
{
    assert( is_inited_ );

    auto it = map_field_id_to_index_.find( field_id );

    if( it == map_field_id_to_index_.end() )
    {
        return nullptr;
    }

    auto & map = it->second;

    auto it2 = map.find( value );

    if( it2 == map.end() )
    {
        return nullptr;
    }

    return it2->second;
}

const Record* Table::find__unlocked( field_id_t field_id, const Value & value ) const
{
    assert( is_inited_ );

    auto it = map_field_id_to_index_.find( field_id );

    if( it == map_field_id_to_index_.end() )
    {
        return nullptr;
    }

    auto & map = it->second;

    auto it2 = map.find( value );

    if( it2 == map.end() )
    {
        return nullptr;
    }

    return it2->second;
}

bool Table::is_matching( const Record & r, const SelectCondition & condition )
{
    Value v;

    if( r.get_field( condition.field_id, & v ) )
    {
        if( anyvalue::compare_values( condition.op, v, condition.value ) )
        {
            return true;
        }
    }

    return false;
}

bool Table::is_matching( const Record & r, bool is_or, const std::vector<SelectCondition> & conditions )
{
    bool has_found_one = false;

    for( auto & c : conditions )
    {
        Value v;

        if( r.get_field( c.field_id, & v ) == false )
        {
            if( is_or )
                continue;
            else
                return false;
        }

        if( anyvalue::compare_values( c.op, v, c.value ) == false )
        {
            if( is_or )
            {
                continue;
            }
            else
                return false;
        }
        else
        {
            if( is_or )
                return true;

            has_found_one   = true;
        }
    }

    return has_found_one;
}

std::vector<Record*> Table::select__unlocked( field_id_t field_id, anyvalue::comparison_type_e op, const Value & value ) const
{
    SelectCondition condition;

    condition.field_id  = field_id;
    condition.op        = op;
    condition.value     = value;

    return select__unlocked( condition );
}

std::vector<Record*> Table::select__unlocked( const SelectCondition & condition ) const
{
    assert( is_inited_ );

    std::vector<Record*>  res;

    for( auto & e : records_ )
    {
        if( is_matching( * e, condition ) )
            res.push_back( e );
    }

    return res;
}

std::vector<Record*> Table::select__unlocked( bool is_or, const std::vector<SelectCondition> & conditions ) const
{
    assert( is_inited_ );

    std::vector<Record*>  res;

    for( auto & e : records_ )
    {
        if( is_matching( * e, is_or, conditions ) )
            res.push_back( e );
    }

    return res;

}

std::mutex & Table::get_mutex() const
{
    return mutex_;
}

bool Table::load_intern( const std::string & filename )
{
    std::ifstream is( filename );

    if( is.fail() )
    {
        dummy_log_warn( MODULENAME, "load_intern: cannot open credentials file %s", filename.c_str() );
        return false;
    }

    Status status;

    auto res = Serializer::load( is, & status );

    if( res == nullptr )
    {
        dummy_log_error( MODULENAME, "load_intern: cannot load credentials" );
        return false;
    }

    std::string error_msg;

    auto b = init_from_status( & error_msg, status );

    if( b == false )
    {
        dummy_log_error( MODULENAME, "load_intern: cannot init login map: %s", error_msg.c_str() );
        return false;
    }

    dummy_log_info( MODULENAME, "load_intern: loaded %d entries from %s, number of keys %u", records_.size(), filename.c_str(), map_field_id_to_index_.size() );

    return true;
}

bool Table::save( std::string * error_msg, const std::string & filename ) const
{
    MUTEX_SCOPE_LOCK( mutex_ );

    assert( is_inited_ );

    auto temp_name  = filename + ".tmp";

    auto b = save_intern( error_msg, temp_name );

    if( b == false )
        return false;

    utils::rename_and_backup( temp_name, filename );

    return true;
}

bool Table::save_intern( std::string * error_msg, const std::string & filename ) const
{
    std::ofstream os( filename );

    if( os.fail() )
    {
        dummy_log_error( MODULENAME, "save_intern: cannot open credentials file %s", filename.c_str() );

        * error_msg =  "cannot open file " + filename;

        return false;
    }

    Status status;

    get_status( & status );

    auto res = Serializer::save( os, status );

    if( res == false )
    {
        dummy_log_error( MODULENAME, "save_intern: cannot save credentials into file %s", filename.c_str()  );

        * error_msg =  "cannot save data into file " + filename;

        return false;
    }

    dummy_log_info( MODULENAME, "save: save %d entries into %s", records_.size(), filename.c_str() );

    return true;
}

void Table::get_status( Status * res ) const
{
    for( auto & e : map_field_id_to_index_ )
    {
        res->index_field_ids.push_back( e.first );
    }

    for( auto & e : records_ )
    {
        res->records.push_back( e );
    }
}

bool Table::init_index(
        const std::vector<field_id_t> & keys )
{
    for( auto & e : keys )
    {
        MapValueIdToRecord x;

        auto b = map_field_id_to_index_.insert( std::make_pair( e, x ) ).second;

        if( b == false )
        {
            dummy_log_error( MODULENAME, "init_index: index %u already exists", e );

            return false;
        }
    }

    return true;
}

bool Table::init_from_status( std::string * error_msg, const Status & status )
{
    init_index( status.index_field_ids );

    for( auto & e : status.records )
    {
        std::string error_msg_2;

        auto b = add_loaded_record__unlocked( e, & error_msg_2 );

        if( b == false )
        {
            * error_msg = "cannot add record " + StrHelper::to_string( * e ) + ": " + error_msg_2;

            return false;
        }
    }

    return true;
}

} // namespace anyvalue_db
