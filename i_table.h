/*

interface ITable.

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

// $Revision: 11801 $ $Date:: 2019-06-21 #$ $Author: serge $

#ifndef ANYVALUE_DB__I_TABLE_H
#define ANYVALUE_DB__I_TABLE_H

#include "types.h"  // field_id_t
#include "value.h"  // Value

namespace anyvalue_db
{

class Record;

class ITable
{
public:

    virtual ~ITable() {};

    virtual bool on_add_field( field_id_t field_id, const Value & value, Record * record )      = 0;
    virtual bool on_update_field( field_id_t field_id, const Value & old_value, const Value & new_value, Record * record )  = 0;
    virtual void on_delete_field( field_id_t field_id, const Value & value )                    = 0;
};

} // namespace anyvalue_db


#endif // ANYVALUE_DB__I_TABLE_H
