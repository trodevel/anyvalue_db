#include <iostream>
#include <string>

#include "db.h"                 // DB
#include "str_helper.h"         // StrHelper
#include "anyvalue/str_helper.h"        // anyvalue::StrHelper

#include "utils/mutex_helper.h"         // MUTEX_SCOPE_LOCK
#include "utils/log_test.h"             // log_test

const int ID            = 1;
const int LOGIN         = 2;
const int PASSWORD      = 3;
const int LAST_NAME     = 4;
const int FIRST_NAME    = 5;
const int EMAIL         = 6;
const int PHONE         = 7;
const int REG_KEY       = 8;
const int TEST_FIELD    = 9;
const int STATUS        = 10;

const int LAST_ID       = 777;
const int CREATOR       = 888;


// for order DB
const int ORDER_ID      = 1;
const int USER_ID       = 2;

bool add_field_if_nonempty(
        anyvalue_db::Record     * record,
        anyvalue_db::field_id_t field_id,
        const std::string       & val )
{
    if( val.empty() )
        return true;

    return record->add_field( field_id, anyvalue::Value( val ) );
}

bool add_field(
        anyvalue_db::Record     * record,
        anyvalue_db::field_id_t field_id,
        int                     val )
{
    return record->add_field( field_id, anyvalue::Value( val ) );
}

anyvalue_db::Record * create_record(
             unsigned           id,
             const std::string  & login,
             const std::string  & password,
             const std::string  & last_name,
             const std::string  & first_name,
             const std::string  & email,
             const std::string  & phone,
             const std::string  & reg_key,
             int                status )
{
    auto res = new anyvalue_db::Record();

    res->add_field( ID, anyvalue::Value( int( id ) ) );

    add_field_if_nonempty( res, LOGIN,          login );
    add_field_if_nonempty( res, PASSWORD,       password );
    add_field_if_nonempty( res, LAST_NAME,      last_name );
    add_field_if_nonempty( res, FIRST_NAME,     first_name );
    add_field_if_nonempty( res, EMAIL,          email );
    add_field_if_nonempty( res, PHONE,          phone );
    add_field_if_nonempty( res, REG_KEY,        reg_key );
    add_field( res, STATUS,                     status );

    return res;
}

bool create_user_in_db(
        anyvalue_db::Table  * table,
        const std::string   & login,
        const std::string   & password,
        const std::string   & last_name,
        const std::string   & first_name,
        const std::string   & email,
        const std::string   & phone,
        const std::string   & reg_key,
        std::string         * error_msg )
{
    auto & mutex = table->get_mutex();

    MUTEX_SCOPE_LOCK( mutex );

    auto res = table->create_record__unlocked( error_msg );

    if( res == nullptr )
        return false;

    res->add_field( LOGIN,          login );
    res->add_field( PASSWORD,       password );
    res->add_field( LAST_NAME,      last_name );
    res->add_field( FIRST_NAME,     first_name );
    res->add_field( EMAIL,          email );
    res->add_field( PHONE,          phone );
    res->add_field( REG_KEY,        reg_key );

    return true;
}

anyvalue_db::Record * create_order(
             unsigned       order_id,
             unsigned       user_id )
{
    auto res = new anyvalue_db::Record();

    res->add_field( ORDER_ID,       int( order_id ) );
    res->add_field( USER_ID,        int( user_id ) );

    return res;
}

anyvalue_db::Record * create_record_1()
{
    return create_record( 1111, "test", "xxx", "Doe", "John", "john.doe@yoyodyne.com", "+1234567890", "afafaf", 2 );
}

anyvalue_db::Record * create_record_2()
{
    return create_record( 2222, "test2", "xxx", "Bowie", "Doris", "doris.bowie@yoyodyne.com", "+9876542310", "jhghjg", 1 );
}

anyvalue_db::Record * create_record_3()
{
    return create_record( 3333, "", "xxx", "Mustemann", "Max", "max.mustermann@yoyodyne.com", "+4930123456", "tyrtyr", 0 );
}

bool create_user_in_db_1( anyvalue_db::Table * table, std::string * error_msg )
{
    return create_user_in_db( table, "test", "xxx", "Doe", "John", "john.doe@yoyodyne.com", "+1234567890", "uhghjg", error_msg );
}

bool create_user_in_db_2( anyvalue_db::Table * table, std::string * error_msg )
{
    return create_user_in_db( table, "test2", "xxx", "Bowie", "Doris", "doris.bowie@yoyodyne.com", "+9876542310", "hjghjgg", error_msg );
}

std::vector<anyvalue_db::Record*> init_table_1( anyvalue_db::Table * table )
{
    std::vector<anyvalue_db::Record*> res;

    res.push_back( create_record_1() );

    table->init( std::vector<anyvalue_db::field_id_t>( { ID, LOGIN, REG_KEY } ));

    std::string error_msg;

    table->add_record( res[ 0 ], & error_msg );

    return res;
}

std::vector<anyvalue_db::Record*> init_table_2( anyvalue_db::Table * table )
{
    std::vector<anyvalue_db::Record*> res;

    res.push_back( create_record_1() );
    res.push_back( create_record_2() );

    table->init( std::vector<anyvalue_db::field_id_t>( { ID, LOGIN, REG_KEY } ));

    std::string error_msg;

    table->add_record( res[ 0 ], & error_msg );
    table->add_record( res[ 1 ], & error_msg );

    return res;
}

std::vector<anyvalue_db::Record*> init_table_3( anyvalue_db::Table * table )
{
    std::vector<anyvalue_db::Record*> res;

    res.push_back( create_record_1() );
    res.push_back( create_record_2() );
    res.push_back( create_record_3() );

    table->init( std::vector<anyvalue_db::field_id_t>( { ID, LOGIN, REG_KEY } ));

    std::string error_msg;

    table->add_record( res[ 0 ], & error_msg );
    table->add_record( res[ 1 ], & error_msg );
    table->add_record( res[ 2 ], & error_msg );

    return res;
}

anyvalue_db::Record * create_order_1()
{
    return create_order( 343434, 1111 );
}

anyvalue_db::Record * create_order_2()
{
    return create_order( 454545, 2222 );
}

anyvalue_db::Record * create_order_3()
{
    return create_order( 565656, 1111 );
}

std::vector<anyvalue_db::Record*> init_order_table_3( anyvalue_db::Table * table )
{
    std::vector<anyvalue_db::Record*> res;

    res.push_back( create_order_1() );
    res.push_back( create_order_2() );
    res.push_back( create_order_3() );

    table->init( std::vector<anyvalue_db::field_id_t>( { ORDER_ID } ));

    std::string error_msg;

    table->add_record( res[ 0 ], & error_msg );
    table->add_record( res[ 1 ], & error_msg );
    table->add_record( res[ 2 ], & error_msg );

    return res;
}

void dump_selection( const std::vector<anyvalue_db::Record*> & vec, const std::string & comment )
{
    std::cout << comment << ":" << "\n";

    for( auto & e : vec )
    {
        std::cout << anyvalue_db::StrHelper::to_string( * e ) << "\n";
    }

    std::cout << "\n";
}

void test_1_add_record_ok_1()
{
    anyvalue_db::Table table;

    table.init( std::vector<anyvalue_db::field_id_t>( { ID, LOGIN } ));

    auto rec = create_record_1();

    std::string error_msg;

    auto b = table.add_record( rec, & error_msg );

    std::cout << anyvalue_db::StrHelper::to_string( table ) << "\n";

    log_test( "test_1_add_record_ok_1", b, true, "record added", "cannot add record", error_msg );
}

void test_1_add_record_nok_1()
{
    anyvalue_db::Table table;

    table.init( std::vector<anyvalue_db::field_id_t>( { ID, LOGIN } ));

    auto rec = create_record_1();

    std::string error_msg;

    auto b = table.add_record( rec, & error_msg );

    b = table.add_record( rec, & error_msg );

    std::cout << anyvalue_db::StrHelper::to_string( table ) << "\n";

    log_test( "test_1_add_record_nok_1", b, false, "the same record was not added", "record was unexpectedly added", error_msg );
}

void test_2()
{
    anyvalue_db::Table table;

    table.init( std::vector<anyvalue_db::field_id_t>( { ID, LOGIN } ));

    {
        auto rec = create_record_1();

        std::string error_msg;

        table.add_record( rec, & error_msg );
    }

    {
        auto rec = create_record_2();

        std::string error_msg;

        auto b = table.add_record( rec, & error_msg );

        std::cout << anyvalue_db::StrHelper::to_string( table ) << "\n";

        log_test( "test_2", b, true, "second record added", "cannot add record", error_msg );
    }
}

void test_3_add()
{
    anyvalue_db::Table table;

    auto recs = init_table_1( & table );

    auto rec = recs[0];

    auto b = rec->add_field( TEST_FIELD, "test_field" );

    std::cout << anyvalue_db::StrHelper::to_string( table ) << "\n";

    log_test( "test_3_add", b, true, "added new field", "cannot add new field", "" );
}

void test_3_add_nok()
{
    anyvalue_db::Table table;

    auto recs = init_table_1( & table );

    auto rec = recs[0];

    auto b = rec->add_field( ID, 3333 );

    std::cout << anyvalue_db::StrHelper::to_string( table ) << "\n";

    log_test( "test_3_add_nok", b, false, "existing field cannot be added", "unexpectedly added an existing field", "" );
}

void test_3_add_nok_2()
{
    anyvalue_db::Table table;

    init_table_1( & table );

    auto rec = create_record_3();

    std::string error_msg;

    table.add_record( rec, & error_msg );

    auto b = rec->add_field( LOGIN, "test" );

    std::cout << anyvalue_db::StrHelper::to_string( table ) << "\n";

    log_test( "test_3_add_nok_2", b, false, "duplicate index field cannot be added", "unexpectedly added an duplicate index field", error_msg );
}

void test_4_modify_ok_1()
{
    anyvalue_db::Table table;

    auto recs = init_table_2( & table );

    auto rec = recs[0];

    auto b = rec->update_field( ID, 5555 );

    std::cout << anyvalue_db::StrHelper::to_string( table ) << "\n";

    log_test( "test_4_modify_ok_1", b, true, "modified existing field", "cannot modify existing field", "" );
}

void test_4_modify_ok_2()
{
    anyvalue_db::Table table;

    auto recs = init_table_2( & table );

    auto rec = recs[0];

    auto b = rec->update_field( LOGIN, "new_login" );

    std::cout << anyvalue_db::StrHelper::to_string( table ) << "\n";

    log_test( "test_4_modify_ok_2", b, true, "modified existing field", "cannot modify existing field", "" );
}

void test_4_modify_nok_1()
{
    anyvalue_db::Table table;

    auto recs = init_table_2( & table );

    auto rec = recs[0];

    auto b = rec->update_field( ID, 2222 );

    std::cout << anyvalue_db::StrHelper::to_string( table ) << "\n";

    log_test( "test_4_modify_nok_1", b, false, "cannot modify existing field", "unexpectedly modified existing field", "" );
}

void test_4_modify_nok_2()
{
    anyvalue_db::Table table;

    auto recs = init_table_2( & table );

    auto rec = recs[0];

    auto b = rec->update_field( LOGIN, "test2" );

    std::cout << anyvalue_db::StrHelper::to_string( table ) << "\n";

    log_test( "test_4_modify_nok_2", b, false, "cannot modify existing field", "unexpectedly modified existing field", "" );
}

void test_5_delete_ok_1()
{
    anyvalue_db::Table table;

    auto recs = init_table_2( & table );

    auto rec = recs[0];

    auto & mutex = table.get_mutex();

    MUTEX_SCOPE_LOCK( mutex );

    std::string error_msg;

    auto b = table.delete_record__unlocked( rec, & error_msg );

    std::cout << anyvalue_db::StrHelper::to_string( table ) << "\n";

    log_test( "test_5_delete_ok_1", b, true, "delete existing record", "cannot delete existing record", error_msg );
}

void test_5_delete_ok_2()
{
    anyvalue_db::Table table;

    init_table_2( & table );

    auto & mutex = table.get_mutex();

    MUTEX_SCOPE_LOCK( mutex );

    std::string error_msg;

    auto b = table.delete_record__unlocked( ID, 2222, & error_msg );

    std::cout << anyvalue_db::StrHelper::to_string( table ) << "\n";

    log_test( "test_5_delete_ok_2", b, true, "delete existing record", "cannot delete existing record", error_msg );
}

void test_5_delete_ok_3()
{
    anyvalue_db::Table table;

    init_table_2( & table );

    auto & mutex = table.get_mutex();

    MUTEX_SCOPE_LOCK( mutex );

    std::string error_msg;

    auto b = table.delete_record__unlocked( LOGIN, "test", & error_msg );

    std::cout << anyvalue_db::StrHelper::to_string( table ) << "\n";

    log_test( "test_5_delete_ok_3", b, true, "delete existing record", "cannot delete existing record", error_msg );
}

void test_5_delete_nok_1()
{
    anyvalue_db::Table table;

    init_table_2( & table );

    auto & mutex = table.get_mutex();

    MUTEX_SCOPE_LOCK( mutex );

    std::string error_msg;

    auto b = table.delete_record__unlocked( nullptr, & error_msg );

    std::cout << anyvalue_db::StrHelper::to_string( table ) << "\n";

    log_test( "test_5_delete_nok_1", b, false, "cannot delete non-existing record", "unexpectedly deleted non-existing record", error_msg );
}

void test_5_delete_nok_2()
{
    anyvalue_db::Table table;

    init_table_2( & table );

    auto & mutex = table.get_mutex();

    MUTEX_SCOPE_LOCK( mutex );

    std::string error_msg;

    auto b = table.delete_record__unlocked( ID, 3333, & error_msg );

    std::cout << anyvalue_db::StrHelper::to_string( table ) << "\n";

    log_test( "test_5_delete_nok_2", b, false, "cannot delete non-existing record", "unexpectedly deleted non-existing record", error_msg );
}

void test_5_delete_nok_3()
{
    anyvalue_db::Table table;

    init_table_2( & table );

    auto & mutex = table.get_mutex();

    MUTEX_SCOPE_LOCK( mutex );

    std::string error_msg;

    auto b = table.delete_record__unlocked( LOGIN, "test3", & error_msg );

    std::cout << anyvalue_db::StrHelper::to_string( table ) << "\n";

    log_test( "test_5_delete_nok_3", b, false, "cannot delete non-existing record", "unexpectedly deleted non-existing record", error_msg );
}

void test_5_delete_nok_4()
{
    anyvalue_db::Table table;

    init_table_2( & table );

    auto & mutex = table.get_mutex();

    MUTEX_SCOPE_LOCK( mutex );

    std::string error_msg;

    auto b = table.delete_record__unlocked( PASSWORD, "xxx", & error_msg );

    std::cout << anyvalue_db::StrHelper::to_string( table ) << "\n";

    log_test( "test_5_delete_nok_4", b, false, "cannot delete non-existing record", "unexpectedly deleted non-existing record", error_msg );
}

void test_6_select_ok_1()
{
    anyvalue_db::Table table;

    auto recs = init_table_3( & table );

    auto & mutex = table.get_mutex();

    MUTEX_SCOPE_LOCK( mutex );

    auto res = table.select__unlocked( LOGIN, anyvalue::comparison_type_e::EQ, "test" );

    std::cout << anyvalue_db::StrHelper::to_string( table ) << "\n";

    dump_selection( res, "test_6_select_ok_1" );

    log_test( "test_6_select_ok_1", res.size() == 1, true, "correct result", "wrong result size", "" );
}

void test_6_select_ok_2()
{
    anyvalue_db::Table table;

    auto recs = init_table_3( & table );

    auto & mutex = table.get_mutex();

    MUTEX_SCOPE_LOCK( mutex );

    auto res = table.select__unlocked( LOGIN, anyvalue::comparison_type_e::NEQ, "test" );

    std::cout << anyvalue_db::StrHelper::to_string( table ) << "\n";

    dump_selection( res, "test_6_select_ok_2" );

    log_test( "test_6_select_ok_2", res.size() == 1, true, "correct result", "wrong result size", "" );
}

void test_7_select_multi_ok_1()
{
    anyvalue_db::Table table;

    auto recs = init_table_3( & table );

    auto & mutex = table.get_mutex();

    MUTEX_SCOPE_LOCK( mutex );

    std::vector<anyvalue_db::Table::SelectCondition> conditions =
    {
            { LOGIN, anyvalue::comparison_type_e::EQ, "test"  },
            { LOGIN, anyvalue::comparison_type_e::EQ, "test2" },
    };

    auto res = table.select__unlocked( true, conditions );

    std::cout << anyvalue_db::StrHelper::to_string( table ) << "\n";

    dump_selection( res, "test_7_select_multi_ok_1" );

    log_test( "test_7_select_multi_ok_1", res.size() == 2, true, "correct result", "wrong result size", "" );
}

void test_7_select_multi_ok_2()
{
    anyvalue_db::Table table;

    auto recs = init_table_3( & table );

    auto & mutex = table.get_mutex();

    MUTEX_SCOPE_LOCK( mutex );

    std::vector<anyvalue_db::Table::SelectCondition> conditions =
    {
            { LOGIN, anyvalue::comparison_type_e::EQ,   "test"  },
            { STATUS, anyvalue::comparison_type_e::EQ,  2 },
    };

    auto res = table.select__unlocked( false, conditions );

    std::cout << anyvalue_db::StrHelper::to_string( table ) << "\n";

    dump_selection( res, "test_7_select_multi_ok_2" );

    log_test( "test_7_select_multi_ok_2", res.size() == 1, true, "correct result", "wrong result size", "" );
}

void test_7_select_multi_ok_3()
{
    anyvalue_db::Table table;

    auto recs = init_table_3( & table );

    auto & mutex = table.get_mutex();

    MUTEX_SCOPE_LOCK( mutex );

    std::vector<anyvalue_db::Table::SelectCondition> conditions =
    {
            { ID, anyvalue::comparison_type_e::EQ,  1111  },
            { ID, anyvalue::comparison_type_e::EQ,  2222  },
            { ID, anyvalue::comparison_type_e::EQ,  3333  },
    };

    auto res = table.select__unlocked( true, conditions );

    std::cout << anyvalue_db::StrHelper::to_string( table ) << "\n";

    dump_selection( res, "test_7_select_multi_ok_3" );

    log_test( "test_7_select_multi_ok_3", res.size() == 3, true, "correct result", "wrong result size", "" );
}

void test_7_select_multi_ok_4()
{
    anyvalue_db::Table table;

    auto recs = init_table_3( & table );

    auto & mutex = table.get_mutex();

    MUTEX_SCOPE_LOCK( mutex );

    std::vector<anyvalue_db::Table::SelectCondition> conditions =
    {
            { PASSWORD, anyvalue::comparison_type_e::EQ,  "xxx" },
            { STATUS, anyvalue::comparison_type_e::NEQ,    1  },
    };

    auto res = table.select__unlocked( false, conditions );

    std::cout << anyvalue_db::StrHelper::to_string( table ) << "\n";

    dump_selection( res, "test_7_select_multi_ok_4" );

    log_test( "test_7_select_multi_ok_4", res.size() == 2, true, "correct result", "wrong result size", "" );
}

void test_8_find_ok_1()
{
    anyvalue_db::Table table;

    auto recs = init_table_3( & table );

    auto & mutex = table.get_mutex();

    MUTEX_SCOPE_LOCK( mutex );

    auto res = table.find__unlocked( ID, 1111 );

    std::cout << anyvalue_db::StrHelper::to_string( table ) << "\n";

    log_test( "test_8_find_ok_1", res != nullptr, true, "found record", "record not found", "" );
}

void test_8_find_ok_2()
{
    anyvalue_db::Table table;

    auto recs = init_table_3( & table );

    auto & mutex = table.get_mutex();

    MUTEX_SCOPE_LOCK( mutex );

    auto res = table.find__unlocked( LOGIN, "test" );

    std::cout << anyvalue_db::StrHelper::to_string( table ) << "\n";

    log_test( "test_8_find_ok_2", res != nullptr, true, "found record", "record not found", "" );
}

void test_8_find_ok_3()
{
    anyvalue_db::Table table;

    auto recs = init_table_3( & table );

    auto & mutex = table.get_mutex();

    MUTEX_SCOPE_LOCK( mutex );

    auto res = table.find__unlocked( REG_KEY, "tyrtyr" );

    std::cout << anyvalue_db::StrHelper::to_string( table ) << "\n";

    log_test( "test_8_find_ok_3", res != nullptr, true, "found record", "record not found", "" );
}

void test_8_find_nok_4()
{
    anyvalue_db::Table table;

    auto recs = init_table_3( & table );

    auto & mutex = table.get_mutex();

    MUTEX_SCOPE_LOCK( mutex );

    auto res = table.find__unlocked( ID, 1234 );

    std::cout << anyvalue_db::StrHelper::to_string( table ) << "\n";

    log_test( "test_8_find_nok_4", res == nullptr, true, "non-existing record not found", "unexpectedly found record", "" );
}

void test_8_find_nok_5()
{
    anyvalue_db::Table table;

    auto recs = init_table_3( & table );

    auto & mutex = table.get_mutex();

    MUTEX_SCOPE_LOCK( mutex );

    auto res = table.find__unlocked( REG_KEY, "blabla" );

    std::cout << anyvalue_db::StrHelper::to_string( table ) << "\n";

    log_test( "test_8_find_nok_5", res == nullptr, true, "non-existing record not found", "unexpectedly found record", "" );
}

void test_8_find_nok_6()
{
    anyvalue_db::Table table;

    auto recs = init_table_3( & table );

    auto & mutex = table.get_mutex();

    MUTEX_SCOPE_LOCK( mutex );

    auto res = table.find__unlocked( STATUS, 0 );

    std::cout << anyvalue_db::StrHelper::to_string( table ) << "\n";

    log_test( "test_8_find_nok_6", res == nullptr, true, "non-index key cannot be used with find()", "unexpectedly found record", "" );
}

void test_8_find_nok_7()
{
    anyvalue_db::Table table;

    auto recs = init_table_3( & table );

    auto & mutex = table.get_mutex();

    MUTEX_SCOPE_LOCK( mutex );

    auto res = table.find__unlocked( FIRST_NAME, "Max" );

    std::cout << anyvalue_db::StrHelper::to_string( table ) << "\n";

    log_test( "test_8_find_nok_7", res == nullptr, true, "non-index key cannot be used with find()", "unexpectedly found record", "" );
}

void test_9_save_ok_1()
{
    anyvalue_db::Table table;

    table.init( std::vector<anyvalue_db::field_id_t>( { ID, LOGIN, REG_KEY } ));

    std::string error_msg;

    auto b = table.save( & error_msg, "test_9_empty.dat" );

    log_test( "test_9_save_ok_1", b, true, "table was written", "cannot write file", error_msg );
}

void test_9_save_ok_2()
{
    anyvalue_db::Table table;

    auto recs = init_table_3( & table );

    std::string error_msg;

    auto b = table.save( & error_msg, "test_9.dat" );

    log_test( "test_9_save_ok_2", b, true, "table was written", "cannot write file", error_msg );
}

void test_10_load_ok_1()
{
    anyvalue_db::Table table;

    auto b = table.init( "test_9_empty.dat" );

    log_test( "test_10_load_ok_1", b, true, "table was loaded", "cannot load table", "" );
}

void test_10_load_ok_2()
{
    anyvalue_db::Table table;

    auto b = table.init( "test_9.dat" );

    log_test( "test_10_load_ok_2", b, true, "table was loaded", "cannot load table", "" );
}

void test_10_load_nok_3()
{
    anyvalue_db::Table table;

    auto b = table.init( "blabla.dat" );

    log_test( "test_10_load_nok_3", b, false, "non-existing table was not loaded", "unexpectedly load non-existing table", "" );
}

void test_11_load_modify_save_ok_1()
{
    anyvalue_db::Table table;

    auto b = table.init( "test_9.dat" );

    if( b == false )
    {
        log_test( "test_11_load_modify_save_ok_1", b, true, "table was loaded", "cannot load table", "" );
        return;
    }

    std::cout << "ORIG:" << "\n" << anyvalue_db::StrHelper::to_string( table ) << "\n";

    {
        auto & mutex = table.get_mutex();

        MUTEX_SCOPE_LOCK( mutex );

        auto rec = table.find__unlocked( LOGIN, "test" );

        if( rec == nullptr )
        {
            log_test( "test_11_load_modify_save_ok_1", rec != nullptr, true, "found record", "cannot find record", "" );
            return;
        }

        b = rec->update_field( LOGIN, "new_login" );

        if( b == false )
        {
            log_test( "test_11_load_modify_save_ok_1", b, true, "updated field", "cannot update field", "" );
            return;
        }
    }

    std::cout << "NEW:" << "\n" << anyvalue_db::StrHelper::to_string( table ) << "\n";

    std::string error_msg;

    b = table.save( & error_msg, "test_11.dat" );

    log_test( "test_11_load_modify_save_ok_1", b, true, "table was written", "cannot write file", error_msg );
}

void test_12_set_metakey_ok_1()
{
    anyvalue_db::Table table;

    auto recs = init_table_1( & table );

    auto rec = recs[0];

    auto b = rec->add_field( TEST_FIELD, "test_field" );

    table.set_meta_key__unlocked( LAST_ID, 123456 );

    std::cout << anyvalue_db::StrHelper::to_string( table ) << "\n";

    log_test( "test_12_set_metakey_ok_1", b, true, "added new field", "cannot add new field", "" );
}

void test_12_set_metakey_ok_2()
{
    anyvalue_db::Table table;

    auto recs = init_table_1( & table );

    auto rec = recs[0];

    auto b = rec->add_field( TEST_FIELD, "test_field" );

    table.set_meta_key__unlocked( LAST_ID, 123456 );
    table.set_meta_key__unlocked( CREATOR, "Mister Jones" );

    std::cout << anyvalue_db::StrHelper::to_string( table ) << "\n";

    log_test( "test_12_set_metakey_ok_2", b, true, "added new field", "cannot add new field", "" );
}

void test_13_get_metakey_ok_1()
{
    anyvalue_db::Table table;

    auto recs = init_table_1( & table );

    auto rec = recs[0];

    rec->add_field( TEST_FIELD, "test_field" );

    table.set_meta_key__unlocked( LAST_ID, 123456 );

    anyvalue::Value val;

    auto b = table.get_meta_key__unlocked( LAST_ID, & val );

    std::cout << anyvalue_db::StrHelper::to_string( table ) << "\n";

    log_test( "test_13_get_metakey_ok_1", b, true, "metakey was found", "cannot find metakey", anyvalue::StrHelper::to_string( val ) );
}

void test_13_get_metakey_nok_1()
{
    anyvalue_db::Table table;

    auto recs = init_table_1( & table );

    auto rec = recs[0];

    rec->add_field( TEST_FIELD, "test_field" );

    anyvalue::Value val;

    auto b = table.get_meta_key__unlocked( LAST_ID, & val );

    std::cout << anyvalue_db::StrHelper::to_string( table ) << "\n";

    log_test( "test_13_get_metakey_nok_1", b, false, "metakey was not found", "metakey was unexpectedly found", anyvalue::StrHelper::to_string( val ) );
}

void test_14_delete_metakey_ok_1()
{
    anyvalue_db::Table table;

    auto recs = init_table_1( & table );

    auto rec = recs[0];

    rec->add_field( TEST_FIELD, "test_field" );

    table.set_meta_key__unlocked( LAST_ID, 123456 );

    auto b = table.delete_meta_key__unlocked( LAST_ID );

    std::cout << anyvalue_db::StrHelper::to_string( table ) << "\n";

    log_test( "test_14_delete_metakey_ok_1", b, true, "deleted metakey", "cannot delete metakey", "" );
}

void test_14_delete_metakey_nok_1()
{
    anyvalue_db::Table table;

    auto recs = init_table_1( & table );

    auto rec = recs[0];

    rec->add_field( TEST_FIELD, "test_field" );

    anyvalue::Value val;

    auto b = table.delete_meta_key__unlocked( LAST_ID );

    std::cout << anyvalue_db::StrHelper::to_string( table ) << "\n";

    log_test( "test_14_delete_metakey_nok_1", b, false, "metakey was not deleted", "metakey was unexpectedly deleted", anyvalue::StrHelper::to_string( val ) );
}

void test_15_save_ok_1()
{
    anyvalue_db::Table table;

    init_table_3( & table );

    table.set_meta_key__unlocked( LAST_ID, 123456 );
    table.set_meta_key__unlocked( CREATOR, "Mister Jones" );

    std::string error_msg;

    auto b = table.save( & error_msg, "test_15.dat" );

    log_test( "test_15_save_ok_1", b, true, "table was written", "cannot write file", error_msg );
}

void test_16_load_modify_save_ok_1()
{
    anyvalue_db::Table table;

    auto b = table.init( "test_15.dat" );

    if( b == false )
    {
        log_test( "test_16_load_modify_save_ok_1", b, true, "table was loaded", "cannot load table", "" );
        return;
    }

    std::cout << "ORIG:" << "\n" << anyvalue_db::StrHelper::to_string( table ) << "\n";

    {
        auto & mutex = table.get_mutex();

        MUTEX_SCOPE_LOCK( mutex );

        table.delete_meta_key__unlocked( LAST_ID );
        table.set_meta_key__unlocked( CREATOR, "Herr Müller" );
    }

    std::cout << "NEW:" << "\n" << anyvalue_db::StrHelper::to_string( table ) << "\n";

    std::string error_msg;

    b = table.save( & error_msg, "test_16.dat" );

    log_test( "test_16_load_modify_save_ok_1", b, true, "table was written", "cannot write file", error_msg );
}

void test_20_add_table_ok_1()
{
    auto * users = new anyvalue_db::Table;

    init_table_3( users );

    anyvalue_db::DB db;

    db.init();

    std::string error_msg;

    auto b = db.add_table( "users", users, & error_msg );

    std::cout << anyvalue_db::StrHelper::to_string( db ) << "\n";

    log_test( "test_20_add_table_ok_1", b, true, "table added", "cannot add table", error_msg );
}

void test_20_add_table_ok_2()
{
    auto * users = new anyvalue_db::Table;

    init_table_3( users );

    auto * orders = new anyvalue_db::Table;

    init_order_table_3( orders );

    anyvalue_db::DB db;

    db.init();

    std::string error_msg;

    auto b = db.add_table( "users", users, & error_msg );
    b &= db.add_table( "orders", orders, & error_msg );

    std::cout << anyvalue_db::StrHelper::to_string( db ) << "\n";

    log_test( "test_20_add_table_ok_2", b, true, "table added", "cannot add table", error_msg );
}

void test_20_add_table_nok_1()
{
    auto * users = new anyvalue_db::Table;

    init_table_3( users );

    anyvalue_db::DB db;

    db.init();

    std::string error_msg;

    db.add_table( "users", users, & error_msg );
    auto b = db.add_table( "users", users, & error_msg );

    std::cout << anyvalue_db::StrHelper::to_string( db ) << "\n";

    log_test( "test_20_add_table_nok_1", b, false, "duplicate table was not added", "unexpectedly added duplicate table", error_msg );
}

void test_21_delete_table_ok_1()
{
    auto * users = new anyvalue_db::Table;

    init_table_3( users );

    auto * orders = new anyvalue_db::Table;

    init_order_table_3( orders );

    anyvalue_db::DB db;

    db.init();

    std::string error_msg;

    db.add_table( "users", users, & error_msg );
    db.add_table( "orders", orders, & error_msg );

    std::cout << "OLD:" << "\n" << anyvalue_db::StrHelper::to_string( db ) << "\n";

    auto b = db.delete_table__unlocked( "orders", & error_msg );

    std::cout << "NEW:" << "\n" << anyvalue_db::StrHelper::to_string( db ) << "\n";

    log_test( "test_21_delete_table_ok_1", b, true, "table deleted", "cannot delete table", error_msg );
}

void test_21_delete_table_nok_1()
{
    anyvalue_db::DB db;

    db.init();

    std::string error_msg;

    auto b = db.delete_table__unlocked( "orders", & error_msg );

    log_test( "test_21_delete_table_nok_1", b, false, "non-existing table was not deleted", "unexpectedly deleted non-existing table", error_msg );
}

void test_22_find_table_ok_1()
{
    auto * users = new anyvalue_db::Table;

    init_table_3( users );

    auto * orders = new anyvalue_db::Table;

    init_order_table_3( orders );

    anyvalue_db::DB db;

    db.init();

    std::string error_msg;

    db.add_table( "users", users, & error_msg );
    db.add_table( "orders", orders, & error_msg );

    auto t = db.find__unlocked( "users" );

    log_test( "test_22_find_table_ok_1", t != nullptr, true, "table found", "cannot find table", "" );
}

void test_22_find_table_nok_1()
{
    anyvalue_db::DB db;

    db.init();

    auto t = db.find__unlocked( "users" );

    log_test( "test_22_find_table_nok_1", t == nullptr, true, "non-existing table was not found", "unexpectedly found non-existing table", "" );
}

void test_23_save_table_ok_1()
{
    auto * users = new anyvalue_db::Table;

    init_table_3( users );

    anyvalue_db::DB db;

    db.init();

    std::string error_msg;

    db.add_table( "users", users, & error_msg );

    auto b = db.save( & error_msg, "test_23_save_table_ok_1.db" );

    log_test( "test_23_save_table_ok_1", b, true, "database saved", "cannot save database", error_msg );
}

void test_23_save_table_ok_2()
{
    auto * users = new anyvalue_db::Table;

    init_table_3( users );

    auto * orders = new anyvalue_db::Table;

    init_order_table_3( orders );

    anyvalue_db::DB db;

    db.init();

    std::string error_msg;

    db.add_table( "users", users, & error_msg );
    db.add_table( "orders", orders, & error_msg );

    auto b = db.save( & error_msg, "test_23_save_table_ok_2.db" );

    log_test( "test_23_save_table_ok_2", b, true, "database saved", "cannot save database", error_msg );
}

void test_24_load_table_ok_1()
{
    anyvalue_db::DB db;

    auto b = db.init( "test_23_save_table_ok_2.db" );

    log_test( "test_24_load_table_ok_1", b, true, "database loaded", "cannot load database", "" );
}

void test_25_load_table_find_table_ok_1()
{
    anyvalue_db::DB db;

    db.init( "test_23_save_table_ok_2.db" );

    auto t = db.find__unlocked( "users" );

    log_test( "test_25_load_table_find_table_ok_1", t != nullptr, true, "table found", "cannot find table", "" );
}

void test_26_load_table_modify_save_ok_1()
{
    anyvalue_db::DB db;

    db.init( "test_23_save_table_ok_2.db" );

    std::string error_msg;

    auto b = db.delete_table__unlocked( "users", & error_msg );

    b &= db.save( & error_msg, "test_26_load_table_modify_save_ok_1.db" );

    log_test( "test_26_load_table_modify_save_ok_1", b, true, "database saved", "cannot save database", error_msg );
}

int main( int argc, const char* argv[] )
{
    test_1_add_record_ok_1();
    test_1_add_record_nok_1();
    test_2();
    test_3_add();
    test_3_add_nok();
    test_3_add_nok_2();
    test_4_modify_ok_1();
    test_4_modify_ok_2();
    test_4_modify_nok_1();
    test_4_modify_nok_2();
    test_5_delete_ok_1();
    test_5_delete_ok_2();
    test_5_delete_ok_3();
    test_5_delete_nok_1();
    test_5_delete_nok_2();
    test_5_delete_nok_3();
    test_5_delete_nok_4();
    test_6_select_ok_1();
    test_6_select_ok_2();
    test_7_select_multi_ok_1();
    test_7_select_multi_ok_2();
    test_7_select_multi_ok_3();
    test_7_select_multi_ok_4();
    test_8_find_ok_1();
    test_8_find_ok_2();
    test_8_find_ok_3();
    test_8_find_nok_4();
    test_8_find_nok_5();
    test_8_find_nok_6();
    test_8_find_nok_7();
    test_9_save_ok_1();
    test_9_save_ok_2();
    test_10_load_ok_1();
    test_10_load_ok_2();
    test_10_load_nok_3();
    test_11_load_modify_save_ok_1();
    test_12_set_metakey_ok_1();
    test_12_set_metakey_ok_2();
    test_13_get_metakey_ok_1();
    test_13_get_metakey_nok_1();
    test_14_delete_metakey_ok_1();
    test_14_delete_metakey_nok_1();
    test_15_save_ok_1();
    test_16_load_modify_save_ok_1();

    test_20_add_table_ok_1();
    test_20_add_table_ok_2();
    test_20_add_table_nok_1();
    test_21_delete_table_ok_1();
    test_21_delete_table_nok_1();
    test_22_find_table_ok_1();
    test_22_find_table_nok_1();
    test_23_save_table_ok_1();
    test_23_save_table_ok_2();
    test_24_load_table_ok_1();
    test_25_load_table_find_table_ok_1();
    test_26_load_table_modify_save_ok_1();

    return 0;
}
