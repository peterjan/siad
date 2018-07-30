package siafile

import (
	"bytes"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"testing"

	"gitlab.com/NebulousLabs/Sia/crypto"
	"gitlab.com/NebulousLabs/Sia/modules"
	"gitlab.com/NebulousLabs/Sia/types"
	"gitlab.com/NebulousLabs/errors"
	"gitlab.com/NebulousLabs/fastrand"
	"gitlab.com/NebulousLabs/writeaheadlog"
)

// rcCode is a dummy implementation of modules.ErasureCoder
type rsCode struct {
	numPieces  int
	dataPieces int
}

func (rs *rsCode) NumPieces() int                                       { return rs.numPieces }
func (rs *rsCode) MinPieces() int                                       { return rs.dataPieces }
func (rs *rsCode) Encode(data []byte) ([][]byte, error)                 { return nil, nil }
func (rs *rsCode) EncodeShards(pieces [][]byte) ([][]byte, error)       { return nil, nil }
func (rs *rsCode) Recover(pieces [][]byte, n uint64, w io.Writer) error { return nil }
func NewRSCode(nData, nParity int) modules.ErasureCoder {
	return &rsCode{
		numPieces:  nData + nParity,
		dataPieces: nData,
	}
}

// AssertEqual asserts that md and md2 are equal and returns an error if they
// are not.
func (md Metadata) AssertEqual(md2 Metadata) error {
	if md.StaticVersion != md2.StaticVersion {
		return errors.New("'staticVersion' of md1 doesn't equal md2's")
	}
	if md.StaticFileSize != md2.StaticFileSize {
		return errors.New("'staticFileSize' of md1 doesn't equal md2's")
	}
	if md.LocalPath != md2.LocalPath {
		return errors.New("'localPath' of md1 doesn't equal md2's")
	}
	if md.SiaPath != md2.SiaPath {
		return errors.New("'siaPath' of md1 doesn't equal md2's")
	}
	if md.StaticMasterKey != md2.StaticMasterKey {
		return errors.New("'staticMasterKey' of md1 doesn't equal md2's")
	}
	if md.StaticSharingKey != md2.StaticSharingKey {
		return errors.New("'staticSharingKey' of md1 doesn't equal md2's")
	}
	if md.ModTime != md2.ModTime {
		return errors.New("'modTime' of md1 doesn't equal md2's")
	}
	if md.ChangeTime != md2.ChangeTime {
		return errors.New("'changeTime' of md1 doesn't equal md2's")
	}
	if md.AccessTime != md2.AccessTime {
		return errors.New("'accessTime' of md1 doesn't equal md2's")
	}
	if md.CreateTime != md2.CreateTime {
		return errors.New("'createTime' of md1 doesn't equal md2's")
	}
	if md.Mode != md2.Mode {
		return errors.New("'mode' of md1 doesn't equal md2's")
	}
	if md.UID != md2.UID {
		return errors.New("'uid' of md1 doesn't equal md2's")
	}
	if md.Gid != md2.Gid {
		return errors.New("'gid' of md1 doesn't equal md2's")
	}
	if md.StaticChunkMetadataSize != md2.StaticChunkMetadataSize {
		return errors.New("'staticChunkMetadataSize' of md1 doesn't equal md2's")
	}
	if md.ChunkOffset != md2.ChunkOffset {
		return errors.New("'chunkOffset' of md1 doesn't equal md2's")
	}
	if md.PubKeyTableOffset != md2.PubKeyTableOffset {
		return errors.New("'pubKeyTableOffset' of md1 doesn't equal md2's")
	}
	return nil
}

// newTestFile is a helper method to create a SiaFile for testing.
func newTestFile() *SiaFile {
	siaPath := string(hex.EncodeToString(fastrand.Bytes(8)))
	rc := NewRSCode(10, 20)
	pieceSize := modules.SectorSize - crypto.TwofishOverhead
	fileSize := pieceSize * 10
	fileMode := os.FileMode(777)
	source := string(hex.EncodeToString(fastrand.Bytes(8)))

	siaFilePath := filepath.Join(os.TempDir(), "siafiles", siaPath)
	sf, err := New(siaFilePath, siaPath, source, newTestWAL(), []modules.ErasureCoder{rc}, pieceSize, fileSize, fileMode)
	if err != nil {
		panic(err)
	}
	return sf
}

// newTestWal is a helper method to create a WAL for testing.
func newTestWAL() *writeaheadlog.WAL {
	// Create the wal.
	walsDir := filepath.Join(os.TempDir(), "wals")
	if err := os.MkdirAll(walsDir, 0700); err != nil {
		panic(err)
	}
	walFilePath := filepath.Join(walsDir, hex.EncodeToString(fastrand.Bytes(8)))
	_, wal, err := writeaheadlog.New(walFilePath)
	if err != nil {
		panic(err)
	}
	return wal
}

// TestCreateReadUpdate tests if an update can be created using createUpdate
// and if the created update can be read using readUpdate.
func TestCreateReadUpdate(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	sf := newTestFile()
	// Create update randomly
	index := int64(fastrand.Intn(100))
	data := fastrand.Bytes(10)
	update := sf.createUpdate(index, data)
	// Read update
	readPath, readIndex, readData, err := readUpdate(update)
	if err != nil {
		t.Fatal("Failed to read update", err)
	}
	// Compare values
	if readPath != sf.siaFilePath {
		t.Error("paths doesn't match")
	}
	if readIndex != index {
		t.Error("index doesn't match")
	}
	if !bytes.Equal(readData, data) {
		t.Error("data doesn't match")
	}
}

// TestApplyUpdates tests a variety of functions that are used to apply
// updates.
func TestApplyUpdates(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	t.Run("TestApplyUpdates", func(t *testing.T) {
		siaFile := newTestFile()
		testApply(t, siaFile, ApplyUpdates)
	})
	t.Run("TestSiaFileApplyUpdates", func(t *testing.T) {
		siaFile := newTestFile()
		testApply(t, siaFile, siaFile.applyUpdates)
	})
	t.Run("TestCreateAndApplyTransaction", func(t *testing.T) {
		siaFile := newTestFile()
		testApply(t, siaFile, siaFile.createAndApplyTransaction)
	})
}

// TestMarshalUnmarshalMetadata tests marshaling and unmarshaling the metadata
// of a SiaFile.
func TestMarshalUnmarshalMetadata(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	sf := newTestFile()

	// Marshal metadata
	raw, err := marshalMetadata(sf.staticMetadata)
	if err != nil {
		t.Fatal("Failed to marshal metadata", err)
	}
	// Unmarshal metadata
	md, err := unmarshalMetadata(raw)
	if err != nil {
		t.Fatal("Failed to unmarshal metadata", err)
	}
	// Compare result to original
	if err := md.AssertEqual(sf.staticMetadata); err != nil {
		t.Fatal("Unmarshaled metadata not equal to marshaled metadata:", err)
	}
}

// TestMarshalUnmarshalMetadata tests marshaling and unmarshaling the
// publicKeyTable of a SiaFile.
func TestMarshalUnmarshalPubKeyTAble(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	sf := newTestFile()
	for i := 0; i < 10; i++ {
		// Create random specifier and key.
		algorithm := types.Specifier{}
		fastrand.Read(algorithm[:])

		// Create random key.
		key := fastrand.Bytes(32)

		// Append new key to slice.
		sf.pubKeyTable = append(sf.pubKeyTable, types.SiaPublicKey{
			Algorithm: algorithm,
			Key:       key,
		})
	}

	// Marshal pubKeyTable.
	raw, err := marshalPubKeyTable(sf.pubKeyTable)
	if err != nil {
		t.Fatal("Failed to marshal pubKeyTable", err)
	}
	// Unmarshal pubKeyTable.
	pubKeyTable, err := unmarshalPubKeyTable(raw)
	if err != nil {
		t.Fatal("Failed to unmarshal pubKeyTable", err)
	}
	// Compare them.
	if len(sf.pubKeyTable) != len(pubKeyTable) {
		t.Fatalf("Lengths of tables don't match %v vs %v",
			len(sf.pubKeyTable), len(pubKeyTable))
	}
	for i, spk := range pubKeyTable {
		if spk.Algorithm != sf.pubKeyTable[i].Algorithm {
			t.Fatal("Algorithms don't match")
		}
		if !bytes.Equal(spk.Key, sf.pubKeyTable[i].Key) {
			t.Fatal("Keys don't match")
		}
	}
}

// testApply tests if a given method applies a set of updates correctly.
func testApply(t *testing.T, siaFile *SiaFile, apply func(...writeaheadlog.Update) error) {
	// Create an update that writes random data to a random index i.
	index := fastrand.Intn(100) + 1
	data := fastrand.Bytes(100)
	update := siaFile.createUpdate(int64(index), data)

	// Apply update.
	if err := apply(update); err != nil {
		t.Fatal("Failed to apply update", err)
	}

	// Check if file has correct size after update.
	file, err := os.Open(siaFile.siaFilePath)
	if err != nil {
		t.Fatal("Failed to open file", err)
	}
	fi, err := file.Stat()
	if err != nil {
		t.Fatal("Failed to get fileinfo", err)
	}
	if fi.Size() != int64(index+len(data)) {
		t.Errorf("File's size should be %v but was %v", index+len(data), fi.Size())
	}

	// Check if correct data was written.
	readData := make([]byte, len(data))
	if _, err := file.ReadAt(readData, int64(index)); err != nil {
		t.Fatal("Failed to read written data back from disk", err)
	}
	if !bytes.Equal(data, readData) {
		t.Fatal("Read data doesn't equal written data")
	}
	// Make sure that we didn't write anything before the specified index.
	readData = make([]byte, index)
	expectedData := make([]byte, index)
	if _, err := file.ReadAt(readData, 0); err != nil {
		t.Fatal("Failed to read data back from disk", err)
	}
	if !bytes.Equal(expectedData, readData) {
		t.Fatal("ApplyUpdates corrupted the data before the specified index")
	}
}
