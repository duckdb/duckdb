    /*
     * m_ps_ denotes field specific to posts
     * m_c_  denotes field specific to comments
     * other m_ fields are common to posts and messages
     * Note: to distinguish between "post" and "comment" records:
     *   - m_c_replyof IS NULL for all "post" records
     *   - m_c_replyof IS NOT NULL for all "comment" records
     */

create table post (
    m_creationdate timestamp without time zone not null,
    m_messageid bigint not null,
    m_ps_imagefile varchar,
    m_locationip varchar not null,
    m_browserused varchar not null,
    m_ps_language varchar,
    m_content text,
    m_length int not null,
    m_creatorid bigint,
    m_ps_forumid bigint,
    m_locationid bigint
);

create table comment (
    m_creationdate timestamp without time zone not null,
    m_messageid bigint not null,
    m_locationip varchar not null,
    m_browserused varchar not null,
    m_content text not null,
    m_length int not null,
    m_creatorid bigint,
    m_locationid bigint,
    m_c_parentpostid bigint,
    m_c_parentcommentid bigint
);

create view message as select m_creationdate, m_messageid, m_ps_imagefile, m_locationip, m_browserused, m_content, 
  m_length, m_creatorid, m_ps_forumid, m_locationid, null as m_c_replyof from post 
  union all select m_creationdate, m_messageid, null as m_ps_imagefile, m_locationip, m_browserused, m_content, 
  m_length, m_creatorid, null as m_ps_forumid, m_locationid, coalesce(m_c_parentpostid, m_c_parentcommentid) m_c_replyof from comment;

create table forum (
   f_creationdate timestamp without time zone not null,
--   f_deletiondate timestamp without time zone not null,
   f_forumid bigint not null,
   f_title varchar not null,
   f_moderatorid bigint
);

create table forum_person (
   fp_creationdate timestamp without time zone not null,
-- fp_deletiondate timestamp without time zone not null,
   fp_forumid bigint not null,
   fp_personid bigint not null
);

create table forum_tag (
   ft_creationdate timestamp without time zone not null,
--   ft_deletiondate timestamp without time zone not null,
   ft_forumid bigint not null,
   ft_tagid bigint not null
);

create table organisation (
   o_organisationid bigint not null,
   o_type varchar not null,
   o_name varchar not null,
   o_url varchar not null,
   o_placeid bigint
);

create table person (
   p_creationdate timestamp without time zone not null,
--   p_deletiondate timestamp without time zone not null,
   p_personid bigint not null,
   p_firstname varchar not null,
   p_lastname varchar not null,
   p_gender varchar not null,
   p_birthday date not null,
   p_locationip varchar not null,
   p_browserused varchar not null,
   p_placeid bigint
);

create table person_email (
   pe_creationdate timestamp without time zone not null,
--   pe_deletiondate timestamp without time zone not null,
   pe_personid bigint not null,
   pe_email varchar not null
);


create table person_tag (
   pt_creationdate timestamp without time zone not null,
--   pt_deletiondate timestamp without time zone not null,
   pt_personid bigint not null,
   pt_tagid bigint not null
);

create table knows (
   k_creationdate timestamp without time zone not null,
--   k_deletiondate timestamp without time zone not null,
   k_person1id bigint not null,
   k_person2id bigint not null
);

create table likes (
   l_creationdate timestamp without time zone not null,
--   l_deletiondate timestamp without time zone not null,
   l_personid bigint not null,
   l_messageid bigint not null
);

create table person_language (
   plang_creationdate timestamp without time zone not null,
--   plang_deletiondate timestamp without time zone not null,
   plang_personid bigint not null,
   plang_language varchar not null
);

create table person_university (
   pu_creationdate timestamp without time zone not null,
--   pu_deletiondate timestamp without time zone not null,
   pu_personid bigint not null,
   pu_organisationid bigint not null,
   pu_classyear int not null
);

create table person_company (
   pc_creationdate timestamp without time zone not null,
--   pc_deletiondate timestamp without time zone not null,
   pc_personid bigint not null,
   pc_organisationid bigint not null,
   pc_workfrom int not null
);

create table place (
   pl_placeid bigint not null,
   pl_name varchar not null,
   pl_url varchar not null,
   pl_type varchar not null,
   pl_containerplaceid bigint
);

create table message_tag (
   mt_creationdate timestamp without time zone not null,
--   mt_deletiondate timestamp without time zone not null,
   mt_messageid bigint not null,
   mt_tagid bigint not null
);

create table tagclass (
   tc_tagclassid bigint not null,
   tc_name varchar not null,
   tc_url varchar not null,
   tc_subclassoftagclassid bigint
);

create table tag (
   t_tagid bigint not null,
   t_name varchar not null,
   t_url varchar not null,
   t_tagclassid bigint not null
);
