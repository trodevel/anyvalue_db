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

// $Revision: 11775 $ $Date:: 2019-06-20 #$ $Author: serge $

#include "table.h"                      // self

#include <fstream>                      // std::ifstream

#include "utils/mutex_helper.h"         // MUTEX_SCOPE_LOCK
#include "utils/dummy_logger.h"         // dummy_log
#include "utils/utils_assert.h"         // ASSERT
#include "utils/rename_and_backup.h"    // utils::rename_and_backup
#include "anyvalue/value_operations.h"  // anyvalue::compare_values
#include "anyvalue/str_helper.h"        // anyvalue::StrHelper

#include "serializer.h"                 // serializer::load

#define MODULENAME      "Table"

namespace anyvalue_db
{

Table::Table()
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

    auto b = load_intern( filename );

    return b;
}

bool Table::init(
        const std::vector<field_id_t> & keys )
{
    MUTEX_SCOPE_LOCK( mutex_ );

    return init_index( keys );
}

Record* Table::create_record__unlocked(
        std::string         * error_msg )
{
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
    auto it = records_.find( record );
    if( it == records_.end() )
    {
        * error_msg   = "record " + std::to_string( (unsigned)record ) + " not found";
        dummy_log_error( MODULENAME, "delete_record__unlocked: record %p not found", record );
        return false;
    }

    records_.erase( it );

    cleanup_index_for_record( record );

    delete record;

    dummy_log_info( MODULENAME, "delete_record__unlocked: record %p deleted", record );

    return true;
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

Record* Table::find__unlocked( field_id_t field_id, const Value & value )
{
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

std::vector<Record*> Table::select__unlocked( field_id_t field_id, anyvalue::comparison_type_e op, const Value & value ) const
{
    std::vector<Record*>  res;

    for( auto & e : records_ )
    {
        Value v;

        if( e->get_field( field_id, & v ) )
        {
            if( anyvalue::compare_values( op, v, value ) )
            {
                res.push_back( e );
            }
        }
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

    dummy_log_info( MODULENAME, "load_intern: loaded %d entries from %s, last id %u", map_id_to_user_.size(), filename.c_str(), status.last_id );

    return true;
}

bool Table::save( std::string * error_msg, const std::string & filename ) const
{
    MUTEX_SCOPE_LOCK( mutex_ );

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

    dummy_log_info( MODULENAME, "save: save %d entries into %s", map_id_to_user_.size(), filename.c_str() );

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

        auto b = add_record__unlocked( e, & error_msg_2 );

        if( b == false )
        {
            * error_msg = "cannot add record " + StrHelper::to_string( e ) + ": " + error_msg_2;

            return false;
        }
    }

    return true;
}

} // namespace anyvalue_db
