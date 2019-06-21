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

// $Revision: 11778 $ $Date:: 2019-06-21 #$ $Author: serge $

#include "str_helper.h"             // self

#include <sstream>                  // std::ostringstream

#include "anyvalue/str_helper.h"    // anyvalue::StrHelper

namespace anyvalue_db
{

#define TUPLE_VAL_STR(_x_)  _x_,#_x_

std::string StrHelper::to_string( const Record & u )
{
    std::ostringstream os;

    for( auto e : u.map_id_to_value_ )
    {
        os << "key_" << e.first << " = " << anyvalue::StrHelper::to_string( e.second ) << " ";
    }

    return os.str();
}

} // namespace anyvalue_db

