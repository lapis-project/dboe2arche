for $db in db:list()[matches(., '^.\d\d\d_.*qdb.*')]
return db:export($db, 'Q:\BaseXdboe\export')