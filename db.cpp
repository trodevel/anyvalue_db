/*

DB.

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

// $Revision: 12071 $ $Date:: 2019-09-27 #$ $Author: serge $

#include "db.h"                      // self

#include <fstream>                      // std::ifstream

#include "utils/mutex_helper.h"         // MUTEX_SCOPE_LOCK
#include "utils/dummy_logger.h"         // dummy_log
#include "utils/utils_assert.h"         // ASSERT
#include "utils/rename_and_backup.h"    // utils::rename_and_backup
#include "anyvalue/value_operations.h"  // anyvalue::compare_values
#include "anyvalue/str_helper.h"        // anyvalue::StrHelper

#include "str_helper.h"                 // StrHelper
#include "serializer.h"                 // serializer::load

#define MODULENAME      "DB"

namespace anyvalue_db
{

DB::DB():
        is_inited_( false )
{
}

DB::~DB()
{
    for( auto e: map_name_to_table_ )
    {
        delete e;
    }
}

bool DB::init(
        const std::string   & filename )
{
    MUTEX_SCOPE_LOCK( mutex_ );

    assert( is_inited_ == false );

    auto b = load_intern( filename );

    if( b )
        is_inited_  = true;

    return b;
}

bool DB::init()
{
    MUTEX_SCOPE_LOCK( mutex_ );

    assert( is_inited_ == false );

    auto b = init_index( keys );

    if( b )
        is_inited_  = true;

    return b;
}

bool DB::add_table(
        const std::string   & name,
        Table               * table,
        std::string         * error_msg )
{
    MUTEX_SCOPE_LOCK( mutex_ );

    return add_table__unlocked( name, table, error_msg );
}

bool DB::add_table__unlocked(
        const std::string   & name,
        Table               * table,
        std::string         * error_msg )
{
    std::string error_msg_2;

    auto b = map_name_to_table_.insert( std::make_pair( name, table ) ).second;

    if( b == false )
    {
        dummy_log_error( MODULENAME, "add_table__unlocked: table %s already exists", name.c_str() );

        * error_msg = "table already exists";

        return false;
    }

    return true;
}


bool DB::delete_table__unlocked(
        const std::string   & name,
        std::string         * error_msg )
{
    assert( is_inited_ );

    auto it = map_name_to_table_.find( name );
    if( it == map_name_to_table_.end() )
    {
        * error_msg   = "table " + name + " not found";
        dummy_log_error( MODULENAME, "delete_table__unlocked: table %s not found", name.c_str() );
        return false;
    }

    auto table = it->second;

    map_name_to_table_.erase( it );

    delete table;

    dummy_log_info( MODULENAME, "delete_table__unlocked: table %s deleted", name.c_str() );

    return true;
}

void DB::set_meta_key(
        metakey_id_t        metakey_id,
        const Value         & value )
{
    MUTEX_SCOPE_LOCK( mutex_ );

    set_meta_key__unlocked( metakey_id, value );
}

void DB::set_meta_key__unlocked(
        metakey_id_t        metakey_id,
        const Value         & value )
{
    map_metakey_id_to_value_[ metakey_id ]    = value;
}

bool DB::get_meta_key(
        metakey_id_t        metakey_id,
        Value               * value )
{
    MUTEX_SCOPE_LOCK( mutex_ );

    return get_meta_key__unlocked( metakey_id, value );
}

bool DB::get_meta_key__unlocked(
        metakey_id_t        metakey_id,
        Value               * value )
{
    auto it = map_metakey_id_to_value_.find( metakey_id );

    if( it == map_metakey_id_to_value_.end() )
        return false;

    * value = it->second;

    return true;
}

bool DB::delete_meta_key(
        metakey_id_t        metakey_id )
{
    MUTEX_SCOPE_LOCK( mutex_ );

    return delete_meta_key__unlocked( metakey_id );
}

bool DB::delete_meta_key__unlocked(
        metakey_id_t        metakey_id )
{
    auto it = map_metakey_id_to_value_.find( metakey_id );

    if( it == map_metakey_id_to_value_.end() )
        return false;

    map_metakey_id_to_value_.erase( it );

    return true;
}

Table* DB::find__unlocked( const std::string & name )
{
    assert( is_inited_ );

    auto it = map_name_to_table_.find( name );
    if( it == map_name_to_table_.end() )
    {
        return nullptr;
    }

    return it->second;
}

const Table* DB::find__unlocked( const std::string & name ) const
{
    assert( is_inited_ );

    auto it = map_name_to_table_.find( name );
    if( it == map_name_to_table_.end() )
    {
        return nullptr;
    }

    return it->second;
}

std::mutex & DB::get_mutex() const
{
    return mutex_;
}

bool DB::load_intern( const std::string & filename )
{
    std::ifstream is( filename );

    if( is.fail() )
    {
        dummy_log_warn( MODULENAME, "load_intern: cannot open credentials file %s", filename.c_str() );
        return false;
    }

    DBStatus status;

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

    dummy_log_info( MODULENAME, "load_intern: loaded %d entries from %s, number of keys %u, number of metakeys %u", map_name_to_table_.size(), filename.c_str(), map_name_to_table_.size(), map_metakey_id_to_value_.size() );

    return true;
}

bool DB::save( std::string * error_msg, const std::string & filename ) const
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

bool DB::save_intern( std::string * error_msg, const std::string & filename ) const
{
    std::ofstream os( filename );

    if( os.fail() )
    {
        dummy_log_error( MODULENAME, "save_intern: cannot open credentials file %s", filename.c_str() );

        * error_msg =  "cannot open file " + filename;

        return false;
    }

    DBStatus status;

    get_status( & status );

    auto res = Serializer::save( os, status );

    if( res == false )
    {
        dummy_log_error( MODULENAME, "save_intern: cannot save credentials into file %s", filename.c_str()  );

        * error_msg =  "cannot save data into file " + filename;

        return false;
    }

    dummy_log_info( MODULENAME, "save: saved %d entries, %d metakeys into %s", map_name_to_table_.size(), map_metakey_id_to_value_.size(), filename.c_str() );

    return true;
}

void DB::get_status( DBStatus * res ) const
{
    res->map_name_to_table = map_name_to_table_

    for( auto & e : map_metakey_id_to_value_ )
    {
        res->metakeys.push_back( std::make_pair( e.first, e.second ) );
    }
}

void DB::init_metakeys_from_status( const DBStatus & status )
{
    for( auto e : status.metakeys )
    {
        map_metakey_id_to_value_.insert( std::make_pair( e.first, e.second ) );
    }
}

bool DB::init_from_status( std::string * error_msg, const DBStatus & status )
{
    for( auto & e : status.map_name_to_table )
    {
        std::string error_msg_2;

        auto b = add_table__unlocked( e.first, e.second, & error_msg_2 );

        if( b == false )
        {
            * error_msg = "cannot add table " + e.first + ": " + error_msg_2;

            return false;
        }
    }

    init_metakeys_from_status( status );

    return true;
}

} // namespace anyvalue_db
