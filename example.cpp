#include <iostream>
#include <string>

#include "table.h"              // Table
#include "str_helper.h"         // StrHelper

#include "utils/mutex_helper.h"         // MUTEX_SCOPE_LOCK

const int LOGIN         = 1;
const int PASSWORD      = 2;
const int LAST_NAME     = 3;
const int FIRST_NAME    = 4;
const int EMAIL         = 5;
const int PHONE         = 6;

bool create_user_1( anyvalue_db::Table * table, std::string * error_msg )
{
    auto & mutex = table->get_mutex();

    MUTEX_SCOPE_LOCK( mutex );

    auto res = table->create_record__unlocked( error_msg );

    if( res == nullptr )
        return false;

    res->add_field( LOGIN,          "test" );
    res->add_field( PASSWORD,       "xxx" );
    res->add_field( LAST_NAME,      "Doe" );
    res->add_field( FIRST_NAME,     "John" );
    res->add_field( EMAIL,          "john.doe@yoyodyne.com" );
    res->add_field( PHONE,          "+1234567890" );

    return true;
}

bool create_user_2( anyvalue_db::Table * table, std::string * error_msg )
{
    auto & mutex = table->get_mutex();

    MUTEX_SCOPE_LOCK( mutex );

    auto res = table->create_record__unlocked( error_msg );

    if( res == nullptr )
        return false;

    res->add_field( LOGIN,          "test2" );
    res->add_field( PASSWORD,       "xxx" );
    res->add_field( LAST_NAME,      "Bowie" );
    res->add_field( FIRST_NAME,     "Doris" );
    res->add_field( EMAIL,          "doris.bowie@yoyodyne.com" );
    res->add_field( PHONE,          "9876542310" );

    return true;
}

#ifdef XXX

void test_2()
{
    anyvalue_db::Table table;

    auto b = table.init( "table.duplicate_id.dat" );

    if( b )
    {
        std::cout << "ERROR: file w/ broken IDs was unexpectedly loaded" << std::endl;
    }
    else
    {
        std::cout << "OK: file w/ broken IDs was not loaded (as expected)" << std::endl;
    }
}

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

    std::string error_msg;

    create_user_1( & table, & error_msg );
    create_user_2( & table, & error_msg );

    auto b = table.save( & error_msg, "test.dat" );

    if( b )
    {
        std::cout << "OK: table was written" << std::endl;
    }
    else
    {
        std::cout << "ERROR: cannot write file: " << error_msg << std::endl;
    }
}

#ifdef XXX
void test_6( const anyvalue_db::Table & table )
{
    std::string error_msg;
    table.save( & error_msg, "users_new.dat" );
}

void test_7()
{
    anyvalue_db::Table table;

    auto b = table.init( "users_new.dat" );

    if( b )
    {
        std::cout << "OK: table was loaded" << std::endl;
    }
    else
    {
        std::cout << "ERROR: cannot load table" << std::endl;
    }
}

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
#ifdef XXX
    anyvalue_db::Table table;

    auto b = table.init( "table.v2.dat" );

    if( b )
    {
        std::cout << "OK: table was loaded" << std::endl;
    }
    else
    {
        std::cout << "ERROR: cannot load table" << std::endl;
        return -1;
    }
#endif

//    test_2();
//    test_3( table );
//    test_4( table );
    test_5();
//    test_6( table );
//    test_7();
//    test_8();

    return 0;
}
