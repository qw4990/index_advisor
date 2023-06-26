CREATE DATABASE imdbload_fk;
USE imdbload_fk;

CREATE TABLE aka_name (
                          id integer NOT NULL PRIMARY KEY,
                          person_id integer NOT NULL,
                          name varchar(512),
                          imdb_index varchar(3),
                          name_pcode_cf varchar(11),
                          name_pcode_nf varchar(11),
                          surname_pcode varchar(11),
                          md5sum varchar(65)
);

CREATE TABLE aka_title (
                           id integer NOT NULL PRIMARY KEY,
                           movie_id integer NOT NULL,
                           title varchar(1000),
                           imdb_index varchar(4),
                           kind_id integer NOT NULL,
                           production_year integer,
                           phonetic_code varchar(5),
                           episode_of_id integer,
                           season_nr integer,
                           episode_nr integer,
                           note varchar(72),
                           md5sum varchar(32)
);

CREATE TABLE cast_info (
                           id integer NOT NULL PRIMARY KEY,
                           person_id integer NOT NULL,
                           movie_id integer NOT NULL,
                           person_role_id integer,
                           note text,
                           nr_order integer,
                           role_id integer NOT NULL
);

CREATE TABLE char_name (
                           id integer NOT NULL PRIMARY KEY,
                           name varchar(512) NOT NULL,
                           imdb_index varchar(2),
                           imdb_id integer,
                           name_pcode_nf varchar(5),
                           surname_pcode varchar(5),
                           md5sum varchar(32)
);

CREATE TABLE comp_cast_type (
                                id integer NOT NULL PRIMARY KEY,
                                kind varchar(32) NOT NULL
);

CREATE TABLE company_name (
                              id integer NOT NULL PRIMARY KEY,
                              name varchar(512) NOT NULL,
                              country_code varchar(6),
                              imdb_id integer,
                              name_pcode_nf varchar(5),
                              name_pcode_sf varchar(5),
                              md5sum varchar(32)
);

CREATE TABLE company_type (
                              id integer NOT NULL PRIMARY KEY,
                              kind varchar(32)
);

CREATE TABLE complete_cast (
                               id integer NOT NULL PRIMARY KEY,
                               movie_id integer,
                               subject_id integer NOT NULL,
                               status_id integer NOT NULL
);

CREATE TABLE info_type (
                           id integer NOT NULL PRIMARY KEY,
                           info varchar(32) NOT NULL
);

CREATE TABLE keyword (
                         id integer NOT NULL PRIMARY KEY,
                         keyword varchar(512) NOT NULL,
                         phonetic_code varchar(5)
);

CREATE TABLE kind_type (
                           id integer NOT NULL PRIMARY KEY,
                           kind varchar(15)
);

CREATE TABLE link_type (
                           id integer NOT NULL PRIMARY KEY,
                           link varchar(32) NOT NULL
);

CREATE TABLE movie_companies (
                                 id integer NOT NULL PRIMARY KEY,
                                 movie_id integer NOT NULL,
                                 company_id integer NOT NULL,
                                 company_type_id integer NOT NULL,
                                 note text
);

CREATE TABLE movie_info_idx (
                                id integer NOT NULL PRIMARY KEY,
                                movie_id integer NOT NULL,
                                info_type_id integer NOT NULL,
                                info text NOT NULL,
                                note text
);

CREATE TABLE movie_keyword (
                               id integer NOT NULL PRIMARY KEY,
                               movie_id integer NOT NULL,
                               keyword_id integer NOT NULL
);

CREATE TABLE movie_link (
                            id integer NOT NULL PRIMARY KEY,
                            movie_id integer NOT NULL,
                            linked_movie_id integer NOT NULL,
                            link_type_id integer NOT NULL
);

CREATE TABLE name (
                      id integer NOT NULL PRIMARY KEY,
                      name varchar(512) NOT NULL,
                      imdb_index varchar(9),
                      imdb_id integer,
                      gender varchar(1),
                      name_pcode_cf varchar(5),
                      name_pcode_nf varchar(5),
                      surname_pcode varchar(5),
                      md5sum varchar(32)
);

CREATE TABLE role_type (
                           id integer NOT NULL PRIMARY KEY,
                           role varchar(32) NOT NULL
);

CREATE TABLE title (
                       id integer NOT NULL PRIMARY KEY,
                       title varchar(512) NOT NULL,
                       imdb_index varchar(5),
                       kind_id integer NOT NULL,
                       production_year integer,
                       imdb_id integer,
                       phonetic_code varchar(5),
                       episode_of_id integer,
                       season_nr integer,
                       episode_nr integer,
                       series_years varchar(49),
                       md5sum varchar(32)
);

CREATE TABLE movie_info (
                            id integer NOT NULL PRIMARY KEY,
                            movie_id integer NOT NULL,
                            info_type_id integer NOT NULL,
                            info text NOT NULL,
                            note text
);

CREATE TABLE person_info (
                             id integer NOT NULL PRIMARY KEY,
                             person_id integer NOT NULL,
                             info_type_id integer NOT NULL,
                             info text NOT NULL,
                             note text
);

create index company_id_movie_companies on movie_companies(company_id);
create index company_type_id_movie_companies on movie_companies(company_type_id);
create index info_type_id_movie_info_idx on movie_info_idx(info_type_id);
create index info_type_id_movie_info on movie_info(info_type_id);
create index info_type_id_person_info on person_info(info_type_id);
create index keyword_id_movie_keyword on movie_keyword(keyword_id);
create index kind_id_aka_title on aka_title(kind_id);
create index kind_id_title on title(kind_id);
create index linked_movie_id_movie_link on movie_link(linked_movie_id);
create index link_type_id_movie_link on movie_link(link_type_id);
create index movie_id_aka_title on aka_title(movie_id);
create index movie_id_cast_info on cast_info(movie_id);
create index movie_id_complete_cast on complete_cast(movie_id);
create index movie_id_movie_companies on movie_companies(movie_id);
create index movie_id_movie_info_idx on movie_info_idx(movie_id);
create index movie_id_movie_keyword on movie_keyword(movie_id);
create index movie_id_movie_link on movie_link(movie_id);
create index movie_id_movie_info on movie_info(movie_id);
create index person_id_aka_name on aka_name(person_id);
create index person_id_cast_info on cast_info(person_id);
create index person_id_person_info on person_info(person_id);
create index person_role_id_cast_info on cast_info(person_role_id);
create index role_id_cast_info on cast_info(role_id);