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

// $Revision: 12068 $ $Date:: 2019-09-27 #$ $Author: serge $

#ifndef ANYVALUE_DB__DB_H
#define ANYVALUE_DB__DB_H

#include <mutex>            // std::mutex
#include <map>              // std::map
#include <set>              // std::set

#include "table.h"          // Table
#include "db_status.h"      // DBStatus

namespace anyvalue_db
{

class DB
{
    friend class Serializer;
    friend class StrHelper;

public:

    DB();
    ~DB();

    bool init(
            const std::string & filename );

    bool init();

    bool add_table(
            const std::string   & name,
            Table               * table,
            std::string         * error_msg );

    bool add_table__unlocked(
            const std::string   & name,
            Table               * table,
            std::string         * error_msg );

    bool delete_table__unlocked(
            const std::string   & name,
            std::string         * error_msg );

    void set_meta_key(
            metakey_id_t        metakey_id,
            const Value         & value );

    bool get_meta_key(
            metakey_id_t        metakey_id,
            Value               * value );

    bool delete_meta_key(
            metakey_id_t        metakey_id );

    void set_meta_key__unlocked(
            metakey_id_t        metakey_id,
            const Value         & value );

    bool get_meta_key__unlocked(
            metakey_id_t        metakey_id,
            Value               * value );

    bool delete_meta_key__unlocked(
            metakey_id_t        metakey_id );

    Table* find__unlocked( const std::string & name );
    const Table* find__unlocked( const std::string & name ) const;

    bool save( std::string * error_msg, const std::string & filename ) const;

    std::mutex & get_mutex() const;

private:

    typedef std::map<std::string,Table*>    MapStringToTable;
    typedef std::map<metakey_id_t,Value>    MapMetaKeyIdToValue;

private:

    bool save_intern( std::string * error_msg, const std::string & filename ) const;
    bool load_intern( const std::string & filename );

    void get_status( DBStatus * res ) const;
    void init_metakeys_from_status( const DBStatus & status );
    bool init_from_status( std::string * error_msg, const DBStatus & status );

private:
    mutable std::mutex          mutex_;

    bool                        is_inited_;

    MapStringToTable            map_name_to_table_;

    MapMetaKeyIdToValue         map_metakey_id_to_value_;
};

} // namespace anyvalue_db


#endif // ANYVALUE_DB__DB_H
