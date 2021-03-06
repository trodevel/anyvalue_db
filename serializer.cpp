/*

Serializer.

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

// $Revision: 12098 $ $Date:: 2019-10-01 #$ $Author: serge $

#include "serializer.h"     // self

#include <stdexcept>        // std::invalid_argument

#include "anyvalue/serializer.h"        // save( ..., anyvalue::Value & )
#include "serializer/serializer.h"      // serializer::
#include "utils/hex_codec.h"            // utils::unhex_string

namespace serializer
{

anyvalue_db::Record* load( std::istream & is, anyvalue_db::Record* e )
{
    if( e != nullptr )
        throw std::invalid_argument( "Serializer::load: e must be null" );

    auto el = anyvalue_db::Serializer::create_Record();

    auto res = anyvalue_db::Serializer::load( is, static_cast< anyvalue_db::Record *>( el ) );

    if( res == nullptr )
    {
        delete el;
        return nullptr;
    }

    return el;
}

anyvalue_db::Record** load( std::istream & is, anyvalue_db::Record** e )
{
    auto res = load( is, static_cast< anyvalue_db::Record *>( nullptr ) );

    if( res == nullptr )
    {
        return nullptr;
    }

    *e =  res;

    return e;
}

bool save( std::ostream & os, const anyvalue_db::Record * e )
{
    return anyvalue_db::Serializer::save( os, * e );
}

anyvalue_db::Table* load( std::istream & is, anyvalue_db::Table* e )
{
    if( e != nullptr )
        throw std::invalid_argument( "Serializer::load: e must be null" );

    auto el = anyvalue_db::Serializer::create_Table();

    auto res = anyvalue_db::Serializer::load( is, static_cast< anyvalue_db::Table *>( el ) );

    if( res == nullptr )
    {
        delete el;
        return nullptr;
    }

    return el;
}

anyvalue_db::Table** load( std::istream & is, anyvalue_db::Table** e )
{
    auto res = load( is, static_cast< anyvalue_db::Table *>( nullptr ) );

    if( res == nullptr )
    {
        return nullptr;
    }

    *e =  res;

    return e;
}

bool save( std::ostream & os, const anyvalue_db::Table * e )
{
    return anyvalue_db::Serializer::save( os, * e );
}
}

namespace anyvalue_db
{

Record* Serializer::create_Record()
{
    return new Record();
}

Table* Serializer::create_Table()
{
    return new Table();
}

Record* Serializer::load_1( std::istream & is, Record* res )
{
    if( res == nullptr )
        throw std::invalid_argument( "Serializer::load: res must not be null" );

    if( serializer::load( is, & res->map_id_to_value_ ) == nullptr )
        return nullptr;

    return res;
}

Record* Serializer::load( std::istream & is, Record* e )
{
    return load_t_1( is, e );
}

bool Serializer::save( std::ostream & os, const Record & e )
{
    static const unsigned int VERSION = 1;

    auto b = serializer::save( os, VERSION );

    if( b == false )
        return false;

    b &= serializer::save( os, e.map_id_to_value_ );

    return b;
}

field_id_t* Serializer::load( std::istream & is, field_id_t* e )
{
    uint32_t    v;

    if( serializer::load( is, & v ) == nullptr )
        return nullptr;

    * e = static_cast<field_id_t>( v );

    return e;
}

//bool Serializer::save( std::ostream & os, const metakey_id_t & e )
//{
//    return serializer::save( os, static_cast<unsigned>( e ) );
//}
//
//metakey_id_t* Serializer::load( std::istream & is, metakey_id_t* e )
//{
//    uint32_t    v;
//
//    if( serializer::load( is, & v ) == nullptr )
//        return nullptr;
//
//    * e = static_cast<metakey_id_t>( v );
//
//    return e;
//}

bool Serializer::save( std::ostream & os, const metakey_id_t & e )
{
    return serializer::save( os, static_cast<unsigned>( e ) );
}

Status* Serializer::load_1( std::istream & is, Status* res )
{
    if( res == nullptr )
        throw std::invalid_argument( "Serializer::load: res must not be null" );

    if( serializer::load( is, & res->index_field_ids ) == nullptr )
        return nullptr;
    if( serializer::load( is, & res->records ) == nullptr )
        return nullptr;
    if( serializer::load( is, & res->metakeys ) == nullptr )
        return nullptr;

    return res;
}

Status* Serializer::load( std::istream & is, Status* e )
{
    return load_t_1( is, e );
}

bool Serializer::save( std::ostream & os, const Status & e )
{
    static const unsigned int VERSION = 1;

    auto b = serializer::save( os, VERSION );

    if( b == false )
        return false;

    b &= serializer::save( os, e.index_field_ids );

    b &= serializer::save<true>( os, e.records );

    b &= serializer::save<true>( os, e.metakeys );

    return b;
}

Table* Serializer::load_1( std::istream & is, Table* res )
{
    if( res == nullptr )
        throw std::invalid_argument( "Serializer::load: res must not be null" );

    Status status;

    if( load( is, & status ) == nullptr )
        return nullptr;

    std::string error_msg;

    auto b = res->init_from_status( & error_msg, status );

    if( b == false )
    {
        //dummy_log_error( MODULENAME, "load_intern: cannot init login map: %s", error_msg.c_str() );
        return nullptr;
    }

    return res;
}

Table* Serializer::load( std::istream & is, Table* e )
{
    return load_t_1( is, e );
}

bool Serializer::save( std::ostream & os, const Table & e )
{
    static const unsigned int VERSION = 1;

    auto b = serializer::save( os, VERSION );

    if( b == false )
        return false;

    Status status;

    e.get_status( & status );

    b &= save( os, status );

    return b;
}

DBStatus* Serializer::load_1( std::istream & is, DBStatus* res )
{
    if( res == nullptr )
        throw std::invalid_argument( "Serializer::load: res must not be null" );

    if( serializer::load( is, & res->map_name_to_table ) == nullptr )
        return nullptr;
    if( serializer::load( is, & res->metakeys ) == nullptr )
        return nullptr;

    return res;
}

DBStatus* Serializer::load( std::istream & is, DBStatus* e )
{
    return load_t_1( is, e );
}

bool Serializer::save( std::ostream & os, const DBStatus & e )
{
    static const unsigned int VERSION = 1;

    auto b = serializer::save( os, VERSION );

    if( b == false )
        return false;

    b &= serializer::save<true>( os, e.map_name_to_table );

    b &= serializer::save<true>( os, e.metakeys );

    return b;
}

} // namespace anyvalue_db
