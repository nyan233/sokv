# sokv
sokv是一个使用经典BTree算法实现的一个key-value存储库, 支持事务, 
多读单写架构, 提供极简的api
## Quick Start
```go
// create file with path is dbset/quick_start
t := sokv.NewBTreeDisk[uint64, string](sokv.Config{
    RootDir:                  "dbset",
    Name:                     "quick_start",
})
err := t.Init()
if err != nil {
    panic(err)
}
// begin tx, write data
// logic exec success after auto commit
err = t.BeginWriteTx(func(tx *sokv.Tx[uint64, string]) error {
    for i := uint64(0); i < 64; i++ {
        _, err = tx.Put(i, strconv.FormatUint(rand.Uint64(), 10))
        if err != nil {
            return err
        }
    }
    return nil
})
if err != nil {
    panic(fmt.Errorf("write tx err:%v", err))
}
// begin tx, read data
err = t.BeginOnlyReadTx(func(tx *sokv.Tx[uint64, string]) error {
    for i := uint64(0); i < 64; i++ {
        k := rand.Uint64N(63)
        v, found, err := tx.Get(k)
        if err != nil {
            return err
        }
        if !found {
            return fmt.Errorf("not found :%d", k)
        }
        fmt.Printf("tree.getVal key=%d, val=%s\n", k, v)
    }
    return nil
})
if err != nil {
    panic(fmt.Errorf("read tx err:%v", err))
}
// close, wait all tx complete
err = t.Close()
if err != nil {
    panic(fmt.Errorf("close err:%v", err))
}
```
## TODO
- [ ] Record Cipher
- [ ] Commited Read
- [ ] Database Stat
- [ ] Optimize Record Log Size
## License
The sokv Use Mit licensed. More is See [Lisence](https://github.com/nyan233/sokv/blob/main/LICENSE)