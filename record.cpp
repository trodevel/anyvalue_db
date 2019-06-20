/*

Record.

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

// $Revision: 11768 $ $Date:: 2019-06-20 #$ $Author: serge $

#include "record.h"     // self

namespace anyvalue_db
{

Record::Record():
        parent_( nullptr )
{
}

Record::Record( ITable * parent ):
        parent_( parent )
{
}

Record::~Record()
{
}

bool Record::has_field( field_id_t field_id ) const
{
    return map_id_to_value_.count( field_id ) > 0;
}

bool Record::get_field( field_id_t field_id, Value * res ) const
{
    auto it = map_id_to_value_.find( field_id );

    if( it == map_id_to_value_.end() )
        return false;

    * res = it->second;

    return true;
}

const Value & Record::get_field( field_id_t field_id ) const
{
    static const Value empty( 0 );

    auto it = map_id_to_value_.find( field_id );

    if( it == map_id_to_value_.end() )
        return empty;

    return it->second;
}

bool Record::add_field( field_id_t field_id, const Value & value )
{
    auto res = map_id_to_value_.insert( std::make_pair( field_id, value ) ).second;

    if( res )
    {
        parent_->on_add_field( field_id, value );
    }

    return res;
}

bool Record::update_field( field_id_t field_id, const Value & value )
{
    auto it = map_id_to_value_.find( field_id );

    if( it == map_id_to_value_.end() )
        return false;

    it->second  = value;

    parent_->on_update_field( field_id, value );

    return true;
}

bool Record::delete_field( field_id_t field_id )
{
    auto res = map_id_to_value_.erase( field_id ) > 0;

    if( res )
    {
        parent_->on_delete_field( field_id );
    }

    return res;
}

} // namespace anyvalue_db
