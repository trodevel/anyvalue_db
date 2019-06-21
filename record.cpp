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

// $Revision: 11788 $ $Date:: 2019-06-21 #$ $Author: serge $

#include "record.h"     // self

#include <cassert>

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

void Record::set_parent( ITable * parent )
{
    assert( parent != nullptr );
    assert( parent_ == nullptr );

    parent_ = parent;
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
    if( map_id_to_value_.count( field_id ) > 0 )
        return false;       // field already exists, cannot insert again

    assert( parent_ );

    if( parent_->on_add_field( field_id, value, this ) == false )   // key already exists
        return false;

    auto b = map_id_to_value_.insert( std::make_pair( field_id, value ) ).second;

    assert( b );

    return true;
}

bool Record::update_field( field_id_t field_id, const Value & value )
{
    auto it = map_id_to_value_.find( field_id );

    if( it == map_id_to_value_.end() )
        return false;       // field doesn't exist, cannot update non-existing field

    assert( parent_ );

    if( parent_->on_update_field( field_id, it->second, value, this ) == false )   // key already exists
        return false;

    it->second  = value;

    return true;
}

bool Record::delete_field( field_id_t field_id )
{
    auto it = map_id_to_value_.find( field_id );

    if( it == map_id_to_value_.end() )
        return false;       // field doesn't exist, cannot delete non-existing field

    assert( parent_ );

    parent_->on_delete_field( field_id, it->second );

    return true;
}

} // namespace anyvalue_db
