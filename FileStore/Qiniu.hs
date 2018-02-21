{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE ScopedTypeVariables #-}
module FileStore.Qiniu where

import ClassyPrelude
-- import qualified Data.ByteString.Lazy       as LB
import qualified Data.ByteString.Char8      as C8
import qualified Data.ByteString.Base64.URL as B64U
import Control.Monad.Except
import Control.Monad.Logger
import Network.Mime                         (MimeType)
import qualified Network.Wreq.Session       as WS

import Data.Byteable                        (Byteable(..))
import System.Random                        (randomIO)
import Qiniu                                as Qiniu

import FileStore.Types


data QiniuFileStore i = QiniuFileStore
                            WS.Session
                            QiniuDualConfig

-- | base64-url-encoded
base64UrlResourceKey :: Byteable a => FilePath -> a -> ResourceKey
base64UrlResourceKey path_prefix = ResourceKey . fromString . (path_prefix </>) . C8.unpack . B64U.encode . toBytes

data QiniuSimpleStat = QiniuSimpleStat
                            Word64
                            MimeType

instance HasFileSize QiniuSimpleStat where
    getFileSize (QiniuSimpleStat fsize _) = fsize

instance HasMimeType QiniuSimpleStat where
    getMimeType (QiniuSimpleStat _ mime) = Just mime


type instance FileStoreIdent (QiniuFileStore i) = i 

type instance FileStoreStat (QiniuFileStore i) = QiniuSimpleStat


-- | The resource key of a file
qiniuFileStoreEntryOfIdent :: Byteable i => QiniuFileStore i -> StorePrivacy -> i -> Qiniu.Entry
qiniuFileStoreEntryOfIdent (QiniuFileStore _sess qc) privacy ident = (bucket, rkey)
  where
    rkey = base64UrlResourceKey (unpack path_prefix) ident

    (bucket, path_prefix) =
      case privacy of
        StorePublic -> (qcDualPublicBucket &&& qcDualPublicPathPrefix)qc
        StorePrivate -> (qcDualPrivateBucket &&& qcDualPrivatePathPrefix) qc


qiniuFileStoreSecretKey :: QiniuFileStore i -> Qiniu.SecretKey
qiniuFileStoreSecretKey (QiniuFileStore _sess qc) = qcDualSecretKey qc


qiniuFileStoreAccessKey :: QiniuFileStore i -> Qiniu.AccessKey
qiniuFileStoreAccessKey (QiniuFileStore _sess qc) = qcDualAccessKey qc


qiniuFileStoreBucket :: QiniuFileStore i -> StorePrivacy -> Bucket
qiniuFileStoreBucket (QiniuFileStore _sess qc) privacy = bucket
  where
    bucket = case privacy of
               StorePublic -> qcDualPublicBucket qc
               StorePrivate -> qcDualPrivateBucket qc


qiniuFileStoreDomain :: QiniuFileStore i -> StorePrivacy -> Maybe Text
qiniuFileStoreDomain (QiniuFileStore _sess qc) privacy = m_domain
  where
    m_domain = case privacy of
                 StorePublic -> qcDualPublicDomain qc
                 StorePrivate -> qcDualPrivateDomain qc


instance
    ( ContentBasedFileIdent i, Byteable i, Eq i
    , MonadIO m, MonadBaseControl IO m, MonadLogger m, MonadCatch m, MonadError String m
    ) =>
    FileStoreService m (QiniuFileStore i)
    where
    fssSaveLBS store@(QiniuFileStore sess qc) m_mime privacy lbs = do
        pp <- mkPutPolicy (Scope bucket Nothing) save_key (fromIntegral (3600*24 :: Int))
        let upload_token = uploadToken skey akey pp
            fp = ""
            region = case privacy of
                       StorePublic -> qcDualPublicRegion qc
                       StorePrivate -> qcDualPrivateRegion qc

        ws_result <- liftM packError $
                        flip runReaderT sess $
                          flip runReaderT (region, upload_token) $
                            uploadOneShot (Just $ rkey) m_mime fp lbs
        case ws_result of
            Left err -> throwError $ either show show err
            Right _  -> return ident
        where
            ident = fileContentIdent lbs
            (bucket, rkey) = qiniuFileStoreEntryOfIdent store privacy ident
            save_key = Nothing
            skey = qcDualSecretKey qc
            akey = qcDualAccessKey qc


    fssDelete store@(QiniuFileStore sess qc) privacy ident = do
        ws_result <- liftM packError $ ioErrorToMonadError $ do
                        flip runReaderT sess $
                          flip runReaderT (skey, akey) $
                            Qiniu.delete (bucket, rkey)
        case ws_result of
            Right _ -> return ()
            Left err | isResourceDoesNotExistError err -> return ()
                     | otherwise -> throwError $ either show show err
        where
            (bucket, rkey) = qiniuFileStoreEntryOfIdent store privacy ident
            skey = qcDualSecretKey qc
            akey = qcDualAccessKey qc


    fssCheckFile store@(QiniuFileStore sess qc) privacy ident = do
        ws_result <- liftM packError $ ioErrorToMonadError $ do
                        flip runReaderT sess $
                          flip runReaderT (skey, akey) $
                            Qiniu.stat (bucket, rkey)
        case ws_result of
            Right _ -> return True
            Left err
                | isResourceDoesNotExistError err -> return False
            Left err -> throwError $ either show show err
        where
            (bucket, rkey) = qiniuFileStoreEntryOfIdent store privacy ident
            skey = qcDualSecretKey qc
            akey = qcDualAccessKey qc


    fssFetchRemoteSaveAs store@(QiniuFileStore sess qc) = Just $ \privacy url ident -> do
        let (bucket, rkey) = qiniuFileStoreEntryOfIdent store privacy ident
        ws_result <- liftM packError $ ioErrorToMonadError $ do
                        flip runReaderT sess $
                          flip runReaderT (skey, akey) $
                            Qiniu.fetch (fromString url) (Scope bucket (Just rkey))
        case ws_result of
            Right _ -> return True
            Left err -> throwError $ either show show err
        where
            skey = qcDualSecretKey qc
            akey = qcDualAccessKey qc


    fssPublicDownloadUrl store@(QiniuFileStore _sess qc) =
        Just $ \ _m_mime ident -> do
                let (bucket, rkey) = qiniuFileStoreEntryOfIdent store StorePublic ident
                    m_domain       = qcDualPublicDomain qc
                    if_ssl         = qcDualPublicSslUrl qc

                return $ resourceDownloadUrl if_ssl m_domain bucket rkey


    fssPrivateDownloadUrl store@(QiniuFileStore _sess qc) =
        Just $ \expiry _m_mime ident -> do
                let (bucket, rkey) = qiniuFileStoreEntryOfIdent store StorePrivate ident
                    m_domain       = qcDualPrivateDomain qc
                    if_ssl         = qcDualPrivateSslUrl qc

                salt :: Word64 <- liftIO randomIO
                let qs = "_r=" <> show salt
                return $ unpack $ authedResourceDownloadUrl' skey akey expiry if_ssl m_domain bucket rkey (Just qs)
                where
                    skey = qcDualSecretKey qc
                    akey = qcDualAccessKey qc


    fssDownloadInternal _ = Nothing


    fssCopyToPublic store@(QiniuFileStore sess qc) ident = do
        ws_result <- liftM packError $ ioErrorToMonadError $ do
                        flip runReaderT sess $
                          flip runReaderT (skey, akey) $
                            Qiniu.copy (pri_bucket, pri_rkey) (pub_bucket, pub_rkey)
        case ws_result of
            Right _ -> return True
            Left err | isResourceDoesNotExistError err -> return False
                     | otherwise -> throwError $ either show show err
        where
            skey = qcDualSecretKey qc
            akey = qcDualAccessKey qc

            (pub_bucket, pub_rkey) = qiniuFileStoreEntryOfIdent store StorePublic ident
            (pri_bucket, pri_rkey) = qiniuFileStoreEntryOfIdent store StorePrivate ident

instance
    ( ContentBasedFileIdent i, Byteable i, Eq i
    , MonadIO m, MonadBaseControl IO m, MonadLogger m, MonadCatch m, MonadError String m
    ) =>
    FileStatService m (QiniuFileStore i)
    where
    fssFileStat store@(QiniuFileStore sess qc) privacy ident = do
        ws_result <- liftM packError $ ioErrorToMonadError $ do
                        flip runReaderT sess $
                          flip runReaderT (skey, akey) $
                            Qiniu.stat (bucket, rkey)
        case ws_result of
            Right st -> return $ Just $
                            QiniuSimpleStat
                                (fromIntegral $ Qiniu.eStatFsize st)
                                (encodeUtf8 $ Qiniu.eStatMimeType st)
            Left err | isResourceDoesNotExistError err -> return Nothing
                     | otherwise -> throwError $ either show show err
        where
            (bucket, rkey) = qiniuFileStoreEntryOfIdent store privacy ident
            skey = qcDualSecretKey qc
            akey = qcDualAccessKey qc
