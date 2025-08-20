lib_LTLIBRARIES += sonic-db-rec/libsonicdbrec.la
sonic_db_rec_libsonicdbrec_la_SOURCES = sonic-db-rec/sonic-db-rec.cpp
sonic_db_rec_libsonicdbrec_la_CFLAGS = $(DBGFLAGS) $(AM_CFLAGS) $(CFLAGS_COMMON)
sonic_db_rec_libsonicdbrec_la_CPPFLAGS = $(DBGFLAGS) $(AM_CPPFLAGS) $(CFLAGS_COMMON)
sonic_db_rec_libsonicdbrec_la_LIBADD = common/libswsscommon.la -lpthread
sonic_db_rec_libsonicdbrec_la_LDFLAGS = -Wl,-z,now $(LDFLAGS)

bin_PROGRAMS += sonic-db-rec/sonic-db-rec
sonic_db_rec_sonic_db_rec_SOURCES = sonic-db-rec/sonic-db-rec.cpp
sonic_db_rec_sonic_db_rec_CFLAGS = $(DBGFLAGS) $(AM_CFLAGS) $(CFLAGS_COMMON)
sonic_db_rec_sonic_db_rec_CPPFLAGS = $(DBGFLAGS) $(AM_CPPFLAGS) $(CFLAGS_COMMON)
sonic_db_rec_sonic_db_rec_LDADD = common/libswsscommon.la -lpthread
sonic_db_rec_sonic_db_rec_LDFLAGS = -Wl,-z,now $(LDFLAGS)
