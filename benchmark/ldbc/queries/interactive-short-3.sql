SELECT p_personid, p_firstname, p_lastname, k_creationdate
FROM knows, person
WHERE k_person1id = 21990232556256
  AND k_person2id = p_personid
ORDER BY k_creationdate DESC, p_personid ASC;
