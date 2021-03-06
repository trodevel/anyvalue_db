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

#ifndef ANYVALUE_DB__SERIALIZER_H
#define ANYVALUE_DB__SERIALIZER_H

#include <iostream>         // std::istream

#include "anyvalue/serializer.h"    // load( ..., anyvalue::Value * )

#include "serializer/versionable_loader_t.h"        // serializer::VersionableLoaderT
#include "record.h"         // Record
#include "status.h"         // Status
#include "db_status.h"      // DBStatus

namespace serializer
{
anyvalue_db::Record** load( std::istream & is, anyvalue_db::Record** e );
bool save( std::ostream & os, const anyvalue_db::Record * e );

anyvalue_db::Table** load( std::istream & is, anyvalue_db::Table** e );
bool save( std::ostream & os, const anyvalue_db::Table * e );
}

namespace anyvalue_db
{

class Serializer: public serializer::VersionableLoaderT<Serializer>
{
    friend serializer::VersionableLoaderT<Serializer>;

public:
    static Record* create_Record();
    static Table* create_Table();

    static Record* load( std::istream & is, Record* e );
    static bool save( std::ostream & os, const Record & e );

    static field_id_t* load( std::istream & is, field_id_t* e );
    static bool save( std::ostream & os, const field_id_t & e );

//    static metakey_id_t* load( std::istream & is, metakey_id_t* e );
//    static bool save( std::ostream & os, const metakey_id_t & e );

    static Status* load( std::istream & is, Status* e );
    static bool save( std::ostream & os, const Status & e );

    static Table* load( std::istream & is, Table* e );
    static bool save( std::ostream & os, const Table & e );

    static DBStatus* load( std::istream & is, DBStatus* e );
    static bool save( std::ostream & os, const DBStatus & e );

private:

    static Record* load_1( std::istream & is, Record* e );

    static Status* load_1( std::istream & is, Status* e );
    static Table* load_1( std::istream & is, Table* e );
    static DBStatus* load_1( std::istream & is, DBStatus* e );
};

} // namespace anyvalue_db

#endif // ANYVALUE_DB__SERIALIZER_H
