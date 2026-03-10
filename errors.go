// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package mariadb

import (
	"fmt"
)

// Error represents a MySQL/MariaDB error
type Error struct {
	Code     uint16
	SQLState string
	Message  string
}

// Error implements the error interface
func (e *Error) Error() string {
	if e.SQLState != "" {
		return fmt.Sprintf("Error %d (%s): %s", e.Code, e.SQLState, e.Message)
	}
	return fmt.Sprintf("Error %d: %s", e.Code, e.Message)
}

// Common error codes
const (
	ER_HASHCHK                    = 1000
	ER_NISAMCHK                   = 1001
	ER_NO                         = 1002
	ER_YES                        = 1003
	ER_CANT_CREATE_FILE           = 1004
	ER_CANT_CREATE_TABLE          = 1005
	ER_CANT_CREATE_DB             = 1006
	ER_DB_CREATE_EXISTS           = 1007
	ER_DB_DROP_EXISTS             = 1008
	ER_DB_DROP_DELETE             = 1009
	ER_DB_DROP_RMDIR              = 1010
	ER_CANT_DELETE_FILE           = 1011
	ER_CANT_FIND_SYSTEM_REC       = 1012
	ER_CANT_GET_STAT              = 1013
	ER_CANT_GET_WD                = 1014
	ER_CANT_LOCK                  = 1015
	ER_CANT_OPEN_FILE             = 1016
	ER_FILE_NOT_FOUND             = 1017
	ER_CANT_READ_DIR              = 1018
	ER_CANT_SET_WD                = 1019
	ER_CHECKREAD                  = 1020
	ER_DISK_FULL                  = 1021
	ER_DUP_KEY                    = 1022
	ER_ERROR_ON_CLOSE             = 1023
	ER_ERROR_ON_READ              = 1024
	ER_ERROR_ON_RENAME            = 1025
	ER_ERROR_ON_WRITE             = 1026
	ER_FILE_USED                  = 1027
	ER_FILSORT_ABORT              = 1028
	ER_FORM_NOT_FOUND             = 1029
	ER_GET_ERRNO                  = 1030
	ER_ILLEGAL_HA                 = 1031
	ER_KEY_NOT_FOUND              = 1032
	ER_NOT_FORM_FILE              = 1033
	ER_NOT_KEYFILE                = 1034
	ER_OLD_KEYFILE                = 1035
	ER_OPEN_AS_READONLY           = 1036
	ER_OUTOFMEMORY                = 1037
	ER_OUT_OF_SORTMEMORY          = 1038
	ER_UNEXPECTED_EOF             = 1039
	ER_CON_COUNT_ERROR            = 1040
	ER_OUT_OF_RESOURCES           = 1041
	ER_BAD_HOST_ERROR             = 1042
	ER_HANDSHAKE_ERROR            = 1043
	ER_DBACCESS_DENIED_ERROR      = 1044
	ER_ACCESS_DENIED_ERROR        = 1045
	ER_NO_DB_ERROR                = 1046
	ER_UNKNOWN_COM_ERROR          = 1047
	ER_BAD_NULL_ERROR             = 1048
	ER_BAD_DB_ERROR               = 1049
	ER_TABLE_EXISTS_ERROR         = 1050
	ER_BAD_TABLE_ERROR            = 1051
	ER_NON_UNIQ_ERROR             = 1052
	ER_SERVER_SHUTDOWN            = 1053
	ER_BAD_FIELD_ERROR            = 1054
	ER_WRONG_FIELD_WITH_GROUP     = 1055
	ER_WRONG_GROUP_FIELD          = 1056
	ER_WRONG_SUM_SELECT           = 1057
	ER_WRONG_VALUE_COUNT          = 1058
	ER_TOO_LONG_IDENT             = 1059
	ER_DUP_FIELDNAME              = 1060
	ER_DUP_KEYNAME                = 1061
	ER_DUP_ENTRY                  = 1062
	ER_WRONG_FIELD_SPEC           = 1063
	ER_PARSE_ERROR                = 1064
	ER_EMPTY_QUERY                = 1065
	ER_NONUNIQ_TABLE              = 1066
	ER_INVALID_DEFAULT            = 1067
	ER_MULTIPLE_PRI_KEY           = 1068
	ER_TOO_MANY_KEYS              = 1069
	ER_TOO_MANY_KEY_PARTS         = 1070
	ER_TOO_LONG_KEY               = 1071
	ER_KEY_COLUMN_DOES_NOT_EXITS  = 1072
	ER_BLOB_USED_AS_KEY           = 1073
	ER_TOO_BIG_FIELDLENGTH        = 1074
	ER_WRONG_AUTO_KEY             = 1075
	ER_READY                      = 1076
	ER_NORMAL_SHUTDOWN            = 1077
	ER_GOT_SIGNAL                 = 1078
	ER_SHUTDOWN_COMPLETE          = 1079
	ER_FORCING_CLOSE              = 1080
	ER_IPSOCK_ERROR               = 1081
	ER_NO_SUCH_INDEX              = 1082
	ER_WRONG_FIELD_TERMINATORS    = 1083
	ER_BLOBS_AND_NO_TERMINATED    = 1084
	ER_TEXTFILE_NOT_READABLE      = 1085
	ER_FILE_EXISTS_ERROR          = 1086
	ER_LOAD_INFO                  = 1087
	ER_ALTER_INFO                 = 1088
	ER_WRONG_SUB_KEY              = 1089
	ER_CANT_REMOVE_ALL_FIELDS     = 1090
	ER_CANT_DROP_FIELD_OR_KEY     = 1091
	ER_INSERT_INFO                = 1092
	ER_UPDATE_TABLE_USED          = 1093
	ER_NO_SUCH_THREAD             = 1094
	ER_KILL_DENIED_ERROR          = 1095
	ER_NO_TABLES_USED             = 1096
	ER_TOO_BIG_SET                = 1097
	ER_NO_UNIQUE_LOGFILE          = 1098
	ER_TABLE_NOT_LOCKED_FOR_WRITE = 1099
	ER_TABLE_NOT_LOCKED           = 1100
)

// Authentication errors
var (
	// ErrCleartextPassword is returned when cleartext password authentication is attempted
	// but AllowCleartextPasswords is false
	ErrCleartextPassword = &Error{
		Code:    2058,
		Message: "cleartext password authentication is not allowed. Set AllowCleartextPasswords=true to enable",
	}

	// ErrNativePassword is returned when native password authentication is attempted
	// but AllowNativePasswords is false
	ErrNativePassword = &Error{
		Code:    2059,
		Message: "native password authentication is not allowed. Set AllowNativePasswords=true to enable",
	}
)
