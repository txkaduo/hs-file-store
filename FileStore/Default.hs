module FileStore.Default where

import ClassyPrelude

import Text.Parsec.TX.Utils


-- | 已知的，能提供 FileStoreService 接口的种类
-- 这不必是完整的列表，只是作为一种识别，方便保存于数据库、文件等
data FileStoreType = FileStoreTypeLocal
                    | FileStoreTypeQiniu
                    deriving (Eq, Ord, Enum, Bounded)

instance SimpleStringRep FileStoreType where
    simpleEncode FileStoreTypeLocal = "local"
    simpleEncode FileStoreTypeQiniu = "qiniu"

    simpleParser = makeSimpleParserByTable
                    [ ( "local", FileStoreTypeLocal)
                    , ( "qiniu", FileStoreTypeQiniu)
                    ]

$(derivePersistFieldS "FileStoreType")
$(deriveJsonS "FileStoreType")
