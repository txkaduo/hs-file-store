{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE FunctionalDependencies #-}
module FileStore.Types where

import ClassyPrelude hiding (try)
import qualified Data.ByteString.Lazy       as LB
import Crypto.Hash.TX.Utils                 (md5HashLBS, MD5Hash, sha256HashLBS, SHA256Hash)

import Control.Monad.Catch                  (try)
import Control.Monad.Except
import Network.Mime                         (MimeType)


-- | 文件标识可以仅仅从文件内容就可以计算出来的情况
-- 比如使用各种hash算法得到的值
class ContentBasedFileIdent a where
    fileContentIdent :: LB.ByteString -> a

instance ContentBasedFileIdent MD5Hash where
    fileContentIdent = md5HashLBS

instance ContentBasedFileIdent SHA256Hash where
    fileContentIdent = sha256HashLBS


class HasFileSize a where
    getFileSize :: a -> Word64

class HasMimeType a where
    getMimeType :: a -> Maybe MimeType


-- | 文件在存储平台的唯一标识
type family FileStoreIdent a :: *

-- | 文件在存储平台内的简单元数据
type family FileStoreStat a :: *

-- | 有㘹空间可能同时支持公开访问及私有访问
-- 对于某些存储服务有意义，假如七牛
data StorePrivacy = StorePrivate | StorePublic
                deriving (Eq, Ord, Enum, Bounded)

-- | 可用于保存文件的某种服务
class (Eq (FileStoreIdent a), Monad m) => FileStoreService m a where

    -- | 保存文件内容
    fssSaveLBS :: a
                -> Maybe MimeType
                -> StorePrivacy
                -> LB.ByteString
                -> m (FileStoreIdent a)

    fssDelete :: a
                 -> StorePrivacy
                 -> FileStoreIdent a
                 -> m ()

    -- | 检查文件是否可用
    fssCheckFile :: a -> StorePrivacy -> FileStoreIdent a -> m Bool

    -- | 从另一个URL上下载
    -- 因为FileStoreIdent a 要知道，通常就是这个文件其实已经在本地下载过
    -- 所以，这个接口的意义主要是为了减少上传带宽
    -- 不支持此功能的平台，可返回 Nothing
    -- 看文档，似乎百度的 BOS，阿里的 OSS 都不支持
    -- 七牛是支持的
    -- CAUTION: 这个接口的含义是平台远程下载另一个URL上的文件
    -- 所以不要通过先下载到本地再上传 （先下载再上传总是可以做的）
    fssFetchRemoteSaveAs :: a
                    -> Maybe (
                        StorePrivacy
                        -> String           -- url
                        -> FileStoreIdent a -- save as
                        -> m Bool           -- false means FileStoreIdent mismatch
                        )
    fssFetchRemoteSaveAs store =
        case fssFetchRemote store of
            Nothing -> Nothing
            Just fetch_func -> Just $ \privacy url ident -> do
                ident' <- fetch_func privacy url
                if ident == ident'
                    then return True
                    else do
                        fssDelete store privacy ident'
                        return False

    -- | 从另一个URL上下载，并能自动计算出正确的 FileStoreIdent
    -- 类似前面的接口，不支持的平台可以返回 Nothing
    -- CAUTION: 这个接口的含义是平台远程下载另一个URL上的文件
    -- 所以不要通过先下载到本地再上传 （先下载再上传总是可以做的）
    fssFetchRemote :: a
                    -> Maybe (
                        StorePrivacy
                        -> String       -- url
                        -> m (FileStoreIdent a)
                        )
    fssFetchRemote _ = Nothing


    -- | 表达这个平台是否有能力提供一个可永久有效的下载某个文件的URL
    -- 所返回的函数，只负责制造URL，不负责检查所指定的文件是否存在
    fssPublicDownloadUrl :: a
                            -> Maybe
                                (Maybe MimeType -> FileStoreIdent a -> m String)

    -- | 表达这个平台是否有能力提供一个可暂时有效的下载某个文件的URL
    -- 所返回的函数，只负责制造URL，不负责检查所指定的文件是否存在
    fssPrivateDownloadUrl :: a
                            -> Maybe
                                (UTCTime -> Maybe MimeType -> FileStoreIdent a -> m String)

    -- | 直接下载文件内容
    -- 这是給可信的调用者使用的接口
    -- 因此不用指定 StorePrivacy
    -- 实现时，尽一切可能返回文件的内容
    fssDownloadInternal :: a
                    -> Maybe (
                        FileStoreIdent a
                        -> m (Maybe LB.ByteString)
                        )

    -- | 把一个文件 copy 至公开空间
    fssCopyToPublic ::  a
                        -> FileStoreIdent a
                        -> m Bool

class (Eq (FileStoreIdent a)) => FileStatService m a where
    fssFileStat :: a
                -> StorePrivacy
                -> FileStoreIdent a
                -> m (Maybe (FileStoreStat a))


data SomeFileStoreService i m =
        forall a.
            ( FileStoreService m a, FileStoreIdent a ~ i
            , FileStatService m a
            , HasFileSize (FileStoreStat a)
            , HasMimeType (FileStoreStat a)
            ) =>
            SomeFileStoreService a


type instance FileStoreIdent (SomeFileStoreService i m) = i

instance (Eq i, Monad m) => FileStoreService m (SomeFileStoreService i m) where
    fssSaveLBS            (SomeFileStoreService x) = fssSaveLBS x
    fssDelete             (SomeFileStoreService x) = fssDelete x
    fssCheckFile          (SomeFileStoreService x) = fssCheckFile x
    fssFetchRemoteSaveAs  (SomeFileStoreService x) = fssFetchRemoteSaveAs x
    fssPublicDownloadUrl  (SomeFileStoreService x) = fssPublicDownloadUrl x
    fssPrivateDownloadUrl (SomeFileStoreService x) = fssPrivateDownloadUrl x
    fssDownloadInternal   (SomeFileStoreService x) = fssDownloadInternal x
    fssCopyToPublic       (SomeFileStoreService x) = fssCopyToPublic x

{--
instance {-# INCOHERENT #-} (FileStoreService m a) => FileStoreService (ReaderT r m) a where
    fssSaveLBS            x y1 y2 y3 = lift $ fssSaveLBS x y1 y2 y3
    fssDelete             x y1 y2 = lift $ fssDelete x y1 y2
    fssCheckFile          x y1 y2 = lift $ fssCheckFile x y1 y2
    fssFetchRemoteSaveAs  x = fmap (\f y1 y2 y3 -> lift $ f y1 y2 y3) $ fssFetchRemoteSaveAs x
    fssPublicDownloadUrl  x = fmap (\f y1 y2 -> lift $ f y1 y2) $ fssPublicDownloadUrl x
    fssPrivateDownloadUrl x = fmap (\f y1 y2 y3 -> lift $ f y1 y2 y3) $ fssPrivateDownloadUrl x
    fssDownloadInternal   x = fmap (\f y1 -> lift $ f y1) $ fssDownloadInternal x
    fssCopyToPublic       x y1 = lift $ fssCopyToPublic x y1

instance {-# INCOHERENT #-} (FileStoreService m a) => FileStoreService (ExceptT e m) a where
    fssSaveLBS            x y1 y2 y3 = lift $ fssSaveLBS x y1 y2 y3
    fssDelete             x y1 y2 = lift $ fssDelete x y1 y2
    fssCheckFile          x y1 y2 = lift $ fssCheckFile x y1 y2
    fssFetchRemoteSaveAs  x = fmap (\f y1 y2 y3 -> lift $ f y1 y2 y3) $ fssFetchRemoteSaveAs x
    fssPublicDownloadUrl  x = fmap (\f y1 y2 -> lift $ f y1 y2) $ fssPublicDownloadUrl x
    fssPrivateDownloadUrl x = fmap (\f y1 y2 y3 -> lift $ f y1 y2 y3) $ fssPrivateDownloadUrl x
    fssDownloadInternal   x = fmap (\f y1 -> lift $ f y1) $ fssDownloadInternal x
    fssCopyToPublic       x y1 = lift $ fssCopyToPublic x y1
-- instance (Eq i, Monad m, FileStoreService m a) => FileStoreService (SqlPersistT m) a where
--}

class HasFileStoreService i m a | a -> i, a -> m where
    getFileStoreService :: a -> SomeFileStoreService i m

instance HasFileStoreService i m (SomeFileStoreService i m) where
    getFileStoreService = id

ioErrorToExceptT :: MonadCatch m => m a -> ExceptT String m a
ioErrorToExceptT f = lift (try f) >>= either (\(e :: IOError) -> throwError $ show e) return

ioErrorToMonadError :: (MonadCatch m, MonadError String m) => m a -> m a
ioErrorToMonadError f = try f >>= either (\(e :: IOError) -> throwError $ show e) return

tryMonadError :: (MonadError e m) => m a -> m (Either e a)
tryMonadError f = liftM Right f `catchError` (return . Left)

tryIOError' :: MonadCatch m => m a -> m (Either IOError a)
tryIOError' = try

