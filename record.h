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

// $Revision: 11783 $ $Date:: 2019-06-21 #$ $Author: serge $

#ifndef ANYVALUE_DB__RECORD_H
#define ANYVALUE_DB__RECORD_H

#include <map>              // std::map
#include "i_table.h"        // ITable

namespace anyvalue_db
{

typedef anyvalue::Value Value;

struct Record
{
    friend class StrHelper;
    friend class Serializer;

    Record( ITable * parent );

    ~Record();

    void set_parent( ITable * parent );

    bool has_field( field_id_t field_id ) const;
    bool get_field( field_id_t field_id, Value * res ) const;
    const Value & get_field( field_id_t field_id ) const;
    bool add_field( field_id_t field_id, const Value & value );
    bool update_field( field_id_t field_id, const Value & value );
    bool delete_field( field_id_t field_id );

private:

    Record(); // for serializer

private:

    typedef std::map<field_id_t,Value> MapIdToValue;

private:

    MapIdToValue    map_id_to_value_;

    ITable          * parent_;
};

} // namespace anyvalue_db


#endif // ANYVALUE_DB__RECORD_H
