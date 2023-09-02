-CREATE PROPERTY GRAPH snb
VERTEX TABLES (
    Person LABEL Person,
    Forum LABEL Forum,
    Organisation LABEL Organisation IN typemask(company, university),
    Place LABEL Place,
    Tag LABEL Tag,
    TagClass LABEL TagClass,
    Country LABEL Country,
    City LABEL City,
    Message LABEL Message
    )
EDGE TABLES (
    Person_knows_person     SOURCE KEY (Person1Id) REFERENCES Person (id)
                            DESTINATION KEY (Person2Id) REFERENCES Person (id)
                            LABEL Knows,
    Forum_hasMember_Person  SOURCE KEY (ForumId) REFERENCES Forum (id)
                            DESTINATION KEY (PersonId) REFERENCES Person (id)
                            LABEL hasMember,
    Forum_hasTag_Tag        SOURCE KEY (ForumId) REFERENCES Forum (id)
                            DESTINATION KEY (TagId) REFERENCES Tag (id)
                            LABEL Forum_hasTag,
    Person_hasInterest_Tag  SOURCE KEY (PersonId) REFERENCES Person (id)
                            DESTINATION KEY (TagId) REFERENCES Tag (id)
                            LABEL hasInterest,
    person_workAt_Organisation SOURCE KEY (PersonId) REFERENCES Person (id)
                               DESTINATION KEY (OrganisationId) REFERENCES Organisation (id)
                               LABEL workAt_Organisation,
    Person_likes_Message    SOURCE KEY (PersonId) REFERENCES Person (id)
                            DESTINATION KEY (id) REFERENCES Message (id)
                            LABEL likes_Message,
    Message_hasTag_Tag      SOURCE KEY (id) REFERENCES Message (id)
                            DESTINATION KEY (TagId) REFERENCES Tag (id)
                            LABEL message_hasTag,
    Message_hasAuthor_Person    SOURCE KEY (messageId) REFERENCES Message (id)
                                DESTINATION KEY (PersonId) REFERENCES Person (id)
                                LABEL hasAuthor,
    Message_replyOf_Message SOURCE KEY (messageId) REFERENCES Message (id)
                            DESTINATION KEY (ParentMessageId) REFERENCES Message (id)
                            LABEL replyOf
    );
