#include <iostream>
#include <string>

#include "table.h"              // Table
#include "str_helper.h"         // StrHelper

#include "utils/mutex_helper.h"         // MUTEX_SCOPE_LOCK

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

    return true;
}

anyvalue_db::Record * create_record_1()
{
    return create_record( 1111, "test", "xxx", "Doe", "John", "john.doe@yoyodyne.com", "+1234567890", "afafaf", 2 );
}

anyvalue_db::Record * create_record_2()
{
    return create_record( 2222, "test2", "xxx", "Bowie", "Doris", "doris.bowie@yoyodyne.com", "+9876542310", "", 1 );
}

anyvalue_db::Record * create_record_3()
{
    return create_record( 3333, "", "xxx", "Mustemann", "Max", "max.mustermann@yoyodyne.com", "+4930123456", "", 0 );
}

bool create_user_in_db_1( anyvalue_db::Table * table, std::string * error_msg )
{
    return create_user_in_db( table, "test", "xxx", "Doe", "John", "john.doe@yoyodyne.com", "+1234567890", error_msg );
}

bool create_user_in_db_2( anyvalue_db::Table * table, std::string * error_msg )
{
    return create_user_in_db( table, "test2", "xxx", "Bowie", "Doris", "doris.bowie@yoyodyne.com", "+9876542310", error_msg );
}

std::vector<anyvalue_db::Record*> init_table_1( anyvalue_db::Table * table )
{
    std::vector<anyvalue_db::Record*> res;

    res.push_back( create_record_1() );

    table->init( std::vector<anyvalue_db::field_id_t>( { ID, LOGIN } ));

    std::string error_msg;

    table->add_record( res[ 0 ], & error_msg );

    return res;
}

std::vector<anyvalue_db::Record*> init_table_2( anyvalue_db::Table * table )
{
    std::vector<anyvalue_db::Record*> res;

    res.push_back( create_record_1() );
    res.push_back( create_record_2() );

    table->init( std::vector<anyvalue_db::field_id_t>( { ID, LOGIN } ));

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

    table->init( std::vector<anyvalue_db::field_id_t>( { ID, LOGIN } ));

    std::string error_msg;

    table->add_record( res[ 0 ], & error_msg );
    table->add_record( res[ 1 ], & error_msg );
    table->add_record( res[ 2 ], & error_msg );

    return res;
}

void log_test(
        const std::string   & test_name,
        bool                res,
        bool                expected_res,
        const std::string   & exp_msg,
        const std::string   & not_exp_msg,
        const std::string   & error_msg )
{
    std::cout << "log_test: "<< test_name << " - ";

    if( res == expected_res )
    {
        std::cout << "OK: " << exp_msg;
    }
    else
    {
        std::cout << "ERROR: " << not_exp_msg;
    }

    if( ! error_msg.empty() )
    {
        std::cout << ": " << error_msg;
    }

    std::cout << std::endl;
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

void test_1()
{
    anyvalue_db::Table table;

    table.init( std::vector<anyvalue_db::field_id_t>( { ID, LOGIN } ));

    auto rec = create_record_1();

    std::string error_msg;

    auto b = table.add_record( rec, & error_msg );

    std::cout << anyvalue_db::StrHelper::to_string( table ) << "\n";

    log_test( "test_1", b, true, "record added", "cannot add record", error_msg );
}

void test_1_nok()
{
    anyvalue_db::Table table;

    table.init( std::vector<anyvalue_db::field_id_t>( { ID, LOGIN } ));

    auto rec = create_record_1();

    std::string error_msg;

    auto b = table.add_record( rec, & error_msg );

    b = table.add_record( rec, & error_msg );

    std::cout << anyvalue_db::StrHelper::to_string( table ) << "\n";

    log_test( "test_1_nok", b, false, "the same record was not added", "record was unexpectedly added", error_msg );
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

#ifdef XXX

void test_3_add( anyvalue_db::Table & table )
{
    auto u = table.find__unlocked( 2849000613 );

    if( u )
    {
        std::cout << "OK: found user: " << anyvalue_db::StrHelper::to_string( * u ) << std::endl;
    }
    else
    {
        std::cout << "ERROR: cannot find user" << std::endl;
    }
}

#endif


void test_5()
{
    anyvalue_db::Table table;

    table.init( std::vector<anyvalue_db::field_id_t>( { ID, LOGIN, REG_KEY } ));

    std::string error_msg;

    create_user_in_db_1( & table, & error_msg );
    create_user_in_db_2( & table, & error_msg );

    auto b = table.save( & error_msg, "test_1.dat" );

    log_test( "test_5", b, true, "table was written", "cannot write file", error_msg );
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

void test_7()
{
    anyvalue_db::Table table;

    auto b = table.init( "test_1.dat" );

    log_test( "test_6", b, true, "table was loaded", "cannot load table", "" );
}

#ifdef XXX
void test_8()
{
    anyvalue_db::Table table;

    auto b = table.init( "table.duplicate_login.dat" );

    if( b )
    {
        std::cout << "ERROR: file w/ duplicate logins was unexpectedly loaded" << std::endl;
    }
    else
    {
        std::cout << "OK: file w/ duplicate logins was not loaded (as expected)" << std::endl;
    }
}
#endif

int main( int argc, const char* argv[] )
{
    test_1();
    test_1_nok();
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
    test_5();
    test_6_select_ok_1();
    test_6_select_ok_2();
    test_7_select_multi_ok_1();
    test_7_select_multi_ok_2();
    test_7_select_multi_ok_3();
    test_7_select_multi_ok_4();
    test_7();
//    test_8();

    return 0;
}
