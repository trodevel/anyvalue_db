/*

AnyvalueDB. Status.

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

// $Revision: 12065 $ $Date:: 2019-09-27 #$ $Author: serge $

#ifndef ANYVALUE_DB__DB_STATUS_H
#define ANYVALUE_DB__DB_STATUS_H

#include <vector>           // std::vector
#include <map>              // std::pair

#include "table.h"          // Table

namespace anyvalue_db
{

struct DBStatus
{
    std::map<std::string,Table*>                map_name_to_table;
    std::vector<std::pair<metakey_id_t,Value>>  metakeys;
};

} // namespace anyvalue_db

#endif // ANYVALUE_DB__DB_STATUS_H
