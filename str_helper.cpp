/*

String Helper. Provides to_string() function.

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

// $Revision: 11817 $ $Date:: 2019-06-23 #$ $Author: serge $

#include "str_helper.h"             // self

#include <sstream>                  // std::ostringstream

#include "anyvalue/str_helper.h"    // anyvalue::StrHelper

namespace anyvalue_db
{

#define TUPLE_VAL_STR(_x_)  _x_,#_x_

std::ostream & StrHelper::write( std::ostream & os, const Record & l )
{
    for( auto e : l.map_id_to_value_ )
    {
        os << "key_" << e.first << " = " << anyvalue::StrHelper::to_string( e.second ) << " ";
    }

    return os;
}

std::ostream & StrHelper::write( std::ostream & os, const Table & l )
{
    os << "index keys: ";
    for( auto e : l.map_field_id_to_index_ )
    {
        os << e.first << " ";
    }

    os << "\n";

    os << "records:" << "\n";

    for( auto e : l.records_ )
    {
        write( os, * e );

        os << "\n";
    }

    return os;
}

} // namespace anyvalue_db

