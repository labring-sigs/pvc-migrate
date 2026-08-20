package objectstore

// The test package uses these adapters to keep behavioral tests outside the
// implementation package while still asserting stable key and error rules.
const ManifestVersionForTest = manifestVersion

func (s *Store) ManifestKeyForTest() string { return s.manifestKey() }

func (s *Store) LockKeyForTest() string { return s.lockKey() }

func IsMissingForTest(err error) bool { return isMissing(err) }
