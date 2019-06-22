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

bool add_field_if_nonempty(
        anyvalue_db::Record     * record,
        anyvalue_db::field_id_t field_id,
        const std::string       & val )
{
    if( val.empty() )
        return true;

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
             const std::string  & reg_key )
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
    return create_record( 1111, "test", "xxx", "Doe", "John", "john.doe@yoyodyne.com", "+1234567890", "afafaf" );
}

anyvalue_db::Record * create_record_2()
{
    return create_record( 2222, "test2", "xxx", "Bowie", "Doris", "doris.bowie@yoyodyne.com", "+9876542310", "" );
}

bool create_user_in_db_1( anyvalue_db::Table * table, std::string * error_msg )
{
    return create_user_in_db( table, "test", "xxx", "Doe", "John", "john.doe@yoyodyne.com", "+1234567890", error_msg );
}

bool create_user_in_db_2( anyvalue_db::Table * table, std::string * error_msg )
{
    return create_user_in_db( table, "test2", "xxx", "Bowie", "Doris", "doris.bowie@yoyodyne.com", "+9876542310", error_msg );
}

void log_test(
        const std::string   & test_name,
        bool                res,
        bool                expected_res,
        const std::string   & exp_msg,
        const std::string   & not_exp_msg,
        const std::string   & error_msg )
{
    std::cout << test_name << " - ";

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


void test_1()
{
    anyvalue_db::Table table;

    table.init( std::vector<anyvalue_db::field_id_t>( { ID, LOGIN } ));

    auto rec = create_record_1();

    std::string error_msg;

    auto b = table.add_record( rec, & error_msg );

    log_test( "test_1", b, true, "record added", "cannot add record", error_msg );
}

void test_2()
{
    anyvalue_db::Table table;

    table.init( std::vector<anyvalue_db::field_id_t>( { ID, LOGIN } ));

    auto rec = create_record_1();

    std::string error_msg;

    auto b = table.add_record( rec, & error_msg );

    b = table.add_record( rec, & error_msg );

    log_test( "test_2", b, false, "the same record was not added", "record was unexpectedly added", error_msg );
}

void test_3()
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

        log_test( "test_3", b, true, "second record added", "cannot add record", error_msg );
    }
}

#ifdef XXX

void test_3( anyvalue_db::Table & table )
{
    auto u = table.find__unlocked( "testuser" );

    if( u )
    {
        std::cout << "OK: found user: " << anyvalue_db::StrHelper::to_string( * u ) << std::endl;
    }
    else
    {
        std::cout << "ERROR: cannot find user" << std::endl;
    }
}

void test_4( anyvalue_db::Table & table )
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

#ifdef XXX
void test_6( const anyvalue_db::Table & table )
{
    std::string error_msg;
    table.save( & error_msg, "users_new.dat" );
}
#endif // XXX

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
    test_2();
    test_3();
//    test_4( table );
    test_5();
//    test_6( table );
    test_7();
//    test_8();

    return 0;
}
