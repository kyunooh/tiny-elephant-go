package tiny_elephant

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	minwise "github.com/dgryski/go-minhash"
	"github.com/go-redis/redis"
	"hash/fnv"
	"log"
	"math/rand"
	"sort"
)

const hashValueSize = 8

// Minhash Prefix
const minPrefix = "min_____"

// Secondary Index Prefix
const siPrefix = "sec__index_____"

type InMemoryCluster struct {
	secondaryIndexDb int
	numOfPerm        int
	seed             int64
	loadDataPer      uint16
	redis            *redis.Client
}

type kv struct {
	Key   string
	Value int
}

type Minhash struct {
	mw   *minwise.MinWise
	seed int64
}

func createHash(seed int64) func(b []byte) uint64 {
	r := rand.New(rand.NewSource(seed))
	b := binary.BigEndian
	b1 := make([]byte, hashValueSize)
	b.PutUint64(b1, uint64(r.Int63()))
	fnv := fnv.New64a()
	h := func(b []byte) uint64 {
		fnv.Reset()
		fnv.Write(b1)
		fnv.Write(b)
		return fnv.Sum64()
	}
	return h
}

func NewMinhash(seed int64, numHash int) *Minhash {
	h1, h2 := createHash(seed), createHash(seed)
	return &Minhash{
		mw:   minwise.NewMinWise(h1, h2, numHash),
		seed: int64(seed),
	}
}

func NewMinhashFromSignature(seed int64, signature []uint64) *Minhash {
	h1, h2 := createHash(seed), createHash(seed)
	return &Minhash{
		mw:   minwise.NewMinWiseFromSignatures(h1, h2, signature),
		seed: int64(seed),
	}
}

func NewInMemoryCluster(redisHost string,
	redisDb int,
	numOfPerm int,
	seed int64,
	loadDataPer uint16,
) InMemoryCluster {
	return InMemoryCluster{
		numOfPerm:   numOfPerm,
		seed:        seed,
		loadDataPer: loadDataPer,
		redis: redis.NewClient(&redis.Options{
			Addr: redisHost,
			DB:   redisDb,
		}),
	}
}

func (cluster *InMemoryCluster) FlushDb() {
	_ = cluster.redis.FlushDB().Err()
}

func (cluster *InMemoryCluster) UpdateCluster(data map[string][]string) {

	for key := range data {
		exists, err := cluster.redis.Exists(minPrefix + key).Result()
		if err != nil {
			fmt.Print(err)
		} else if exists != 0 {
			cluster.updateSecondaryIndex(key, data[key])
		} else {
			cluster.generateAndSaveMinhash(key, data[key])
			cluster.generateAndSaveSecondaryIndex(key)
		}
	}
}

func (cluster *InMemoryCluster) updateSecondaryIndex(key string, streams []string) {
	orgHashes := cluster.getHashesFromRedis(key)
	var copyHashes []uint64
	copyHashes = append(copyHashes, orgHashes...)
	minhash := NewMinhashFromSignature(cluster.seed, orgHashes)
	for _, stream := range streams {
		minhash.mw.Push([]byte(stream))
	}
	updatedHashes := minhash.mw.Signature()
	pipe := cluster.redis.TxPipeline()
	for i, hash := range copyHashes {
		updatedHash := updatedHashes[i]
		if hash != updatedHash {
			orgSecondaryKey := fmt.Sprintf(siPrefix+"%v-%v", i, hash)
			pipe.LRem(orgSecondaryKey, 1, key)
			updatedSecondaryKey := fmt.Sprintf(siPrefix+"%v-%v", i, updatedHashes[i])
			pipe.LPush(updatedSecondaryKey, key)
		}
	}
	signatureJson, _ := json.Marshal(minhash.mw.Signature())
	err := pipe.Set(minPrefix+key, signatureJson, 0).Err()
	if err != nil {
		panic(err)
	}
	_, _ = pipe.Exec()

}

func (cluster *InMemoryCluster) MostCommon(key string, top int) []kv {
	hashes := cluster.getHashesFromRedis(key)
	var keys []string

	for i, hash := range hashes {
		secondaryKey := fmt.Sprintf(siPrefix+"%v-%v", i, hash)
		keysInSecondaryIndex := cluster.redis.LRange(secondaryKey, 0, -1).Val()
		keys = append(keys, keysInSecondaryIndex...)
	}

	counts := make(map[string]int)
	for _, key := range keys {
		count := (counts)[key]
		(counts)[key] = count + 1
	}

	var ss []kv
	for k, v := range counts {
		ss = append(ss, kv{k, v})
	}
	sort.Slice(ss, func(i, j int) bool {
		return ss[i].Value > ss[j].Value
	})
	ssLen := len(ss)
	if ssLen < 2 {
		return make([]kv, 0)
	} else if ssLen <= top {
		return ss[1:ssLen]
	}
	return ss[1 : top+1]
}

func (cluster *InMemoryCluster) generateAndSaveMinhash(key string, streams []string) {
	minhash := NewMinhash(cluster.seed, cluster.numOfPerm)
	for _, stream := range streams {
		minhash.mw.Push([]byte(stream))
	}
	signatureJson, _ := json.Marshal(minhash.mw.Signature())
	err := cluster.redis.Set(minPrefix+key, signatureJson, 0).Err()
	if err != nil {
		panic(err)
	}
}

func (cluster *InMemoryCluster) getHashesFromRedis(key string) []uint64 {
	hashesJson, getValueErr := cluster.redis.Get(minPrefix + key).Bytes()
	if getValueErr != nil {
		log.Fatal(getValueErr)
	}
	hashes := *new([]uint64)
	marshallErr := json.Unmarshal(hashesJson, &hashes)
	if marshallErr != nil {
		fmt.Println(marshallErr)
	}
	return hashes
}

func (cluster *InMemoryCluster) generateAndSaveSecondaryIndex(key string) {
	hashes := cluster.getHashesFromRedis(key)
	for i, hash := range hashes {
		secondaryKey := fmt.Sprintf("sec__index_____%v-%v", i, hash)
		cluster.redis.LPush(secondaryKey, key)
	}
}
