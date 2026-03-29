/*
     * m_ps_ denotes field specific to posts
     * m_c_  denotes field specific to comments
     * other m_ fields are common to posts and messages
     * Note: to distinguish between "post" and "comment" records:
     *   - m_c_replyof IS NULL for all "post" records
     *   - m_c_replyof IS NOT NULL for all "comment" records
     */
CREATE TABLE post(
    m_creationdate timestamp without time zone NOT NULL,
    m_messageid bigint NOT NULL,
    m_ps_imagefile varchar,
    m_locationip varchar NOT NULL,
    m_browserused varchar NOT NULL,
    m_ps_language varchar,
    m_content text,
    m_length int NOT NULL,
    m_creatorid bigint,
    m_ps_forumid bigint,
    m_locationid bigint
);

CREATE TABLE comment (
    m_creationdate timestamp without time zone NOT NULL,
    m_messageid bigint NOT NULL,
    m_locationip varchar NOT NULL,
    m_browserused varchar NOT NULL,
    m_content text NOT NULL,
    m_length int NOT NULL,
    m_creatorid bigint,
    m_locationid bigint,
    m_c_parentpostid bigint,
    m_c_parentcommentid bigint
);

CREATE VIEW message AS
SELECT
    m_creationdate,
    m_messageid,
    m_ps_imagefile,
    m_locationip,
    m_browserused,
    m_content,
    m_length,
    m_creatorid,
    m_ps_forumid,
    m_locationid,
    NULL AS m_c_replyof
FROM post
UNION ALL
SELECT
    m_creationdate,
    m_messageid,
    NULL AS m_ps_imagefile,
    m_locationip,
    m_browserused,
    m_content,
    m_length,
    m_creatorid,
    NULL AS m_ps_forumid,
    m_locationid,
    coalesce(m_c_parentpostid, m_c_parentcommentid) m_c_replyof
FROM comment;

CREATE TABLE forum(f_creationdate timestamp without time zone NOT NULL, --   f_deletiondate timestamp without time zone not null,
f_forumid bigint NOT NULL, f_title varchar NOT NULL, f_moderatorid bigint);

CREATE TABLE forum_person(fp_creationdate timestamp without time zone NOT NULL, -- fp_deletiondate timestamp without time zone not null,
fp_forumid bigint NOT NULL, fp_personid bigint NOT NULL);

CREATE TABLE forum_tag(ft_creationdate timestamp without time zone NOT NULL, --   ft_deletiondate timestamp without time zone not null,
ft_forumid bigint NOT NULL, ft_tagid bigint NOT NULL);

CREATE TABLE organisation(
    o_organisationid bigint NOT NULL,
    o_type varchar NOT NULL,
    o_name varchar NOT NULL,
    o_url varchar NOT NULL,
    o_placeid bigint
);

CREATE TABLE person(p_creationdate timestamp without time zone NOT NULL, --   p_deletiondate timestamp without time zone not null,
p_personid bigint NOT NULL, p_firstname varchar NOT NULL, p_lastname varchar NOT NULL, p_gender varchar NOT NULL, p_birthday date NOT NULL, p_locationip varchar NOT NULL, p_browserused varchar NOT NULL, p_placeid bigint);

CREATE TABLE person_email(pe_creationdate timestamp without time zone NOT NULL, --   pe_deletiondate timestamp without time zone not null,
pe_personid bigint NOT NULL, pe_email varchar NOT NULL);

CREATE TABLE person_tag(pt_creationdate timestamp without time zone NOT NULL, --   pt_deletiondate timestamp without time zone not null,
pt_personid bigint NOT NULL, pt_tagid bigint NOT NULL);

CREATE TABLE knows(k_creationdate timestamp without time zone NOT NULL, --   k_deletiondate timestamp without time zone not null,
k_person1id bigint NOT NULL, k_person2id bigint NOT NULL);

CREATE TABLE likes(l_creationdate timestamp without time zone NOT NULL, --   l_deletiondate timestamp without time zone not null,
l_personid bigint NOT NULL, l_messageid bigint NOT NULL);

CREATE TABLE person_language(plang_creationdate timestamp without time zone NOT NULL, --   plang_deletiondate timestamp without time zone not null,
plang_personid bigint NOT NULL, plang_language varchar NOT NULL);

CREATE TABLE person_university(pu_creationdate timestamp without time zone NOT NULL, --   pu_deletiondate timestamp without time zone not null,
pu_personid bigint NOT NULL, pu_organisationid bigint NOT NULL, pu_classyear int NOT NULL);

CREATE TABLE person_company(pc_creationdate timestamp without time zone NOT NULL, --   pc_deletiondate timestamp without time zone not null,
pc_personid bigint NOT NULL, pc_organisationid bigint NOT NULL, pc_workfrom int NOT NULL);

CREATE TABLE place(
    pl_placeid bigint NOT NULL,
    pl_name varchar NOT NULL,
    pl_url varchar NOT NULL,
    pl_type varchar NOT NULL,
    pl_containerplaceid bigint
);

CREATE TABLE message_tag(mt_creationdate timestamp without time zone NOT NULL, --   mt_deletiondate timestamp without time zone not null,
mt_messageid bigint NOT NULL, mt_tagid bigint NOT NULL);

CREATE TABLE tagclass(
    tc_tagclassid bigint NOT NULL,
    tc_name varchar NOT NULL,
    tc_url varchar NOT NULL,
    tc_subclassoftagclassid bigint
);

CREATE TABLE tag(
    t_tagid bigint NOT NULL,
    t_name varchar NOT NULL,
    t_url varchar NOT NULL,
    t_tagclassid bigint NOT NULL
);
