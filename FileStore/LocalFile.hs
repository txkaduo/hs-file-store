{-# LANGUAGE UndecidableInstances #-}
module FileStore.LocalFile where

import ClassyPrelude
import qualified Data.ByteString.Lazy       as LB
import qualified Data.ByteString.Char8      as C8
import qualified Data.ByteString.Base64.URL as B64U
import Control.Monad.Except
import Control.Monad.Logger
import System.Directory                     (doesFileExist, createDirectoryIfMissing, removeFile)
import System.FilePath                      (takeDirectory)
import System.Posix.Files                   (getFileStatus, fileSize)
import Network.Mime                         (MimeType)

import Data.Byteable                        (Byteable(..))

#if MIN_VERSION_classy_prelude(1, 5, 0)
import Control.Monad.Catch                  (MonadCatch)
#endif

import FileStore.Types


data LocalFileStore i = LocalFileStore
  { localFileStoreBaseDir        :: FilePath                                              -- ^ base dir
  , localFileStorePublicUrl      :: (Maybe (Maybe MimeType -> i -> IO String))            -- ^ to make public download url
  , localFileStoreTimeLimitedUrl :: (Maybe (UTCTime -> Maybe MimeType -> i -> IO String)) -- ^ to make time-limited download url
  }

-- | base64-url-encoded
base64UrlFilePath :: (Byteable a, IsString s) => a -> s
base64UrlFilePath = fromString . C8.unpack . B64U.encodeBase64' . toBytes

data LocalFileStat = LocalFileStat Word64

instance HasFileSize LocalFileStat where
    getFileSize (LocalFileStat fsize) = fsize

instance HasMimeType LocalFileStat where
    getMimeType _ = Nothing

type instance FileStoreIdent (LocalFileStore i) = i

type instance FileStoreStat (LocalFileStore i) = LocalFileStat

instance
    ( ContentBasedFileIdent i, Byteable i, Eq i
    , MonadIO m, MonadLogger m, MonadCatch m, MonadError String m
    ) =>
    FileStoreService m (LocalFileStore i)
    where
    fssSaveLBS store@(LocalFileStore base_dir _ _) _ privacy lbs = do
        exists <- fssCheckFile store privacy ident

        unless exists $ ioErrorToMonadError $ liftIO $ do
            createDirectoryIfMissing True $ takeDirectory fp
            LB.writeFile fp lbs

        return ident

        where
            ident   = fileContentIdent lbs
            fp      = base_dir </> base64UrlFilePath ident

    fssDelete (LocalFileStore base_dir _ _) _privacy ident = ioErrorToMonadError $ do
        err_or <- liftIO $ tryIOError $ removeFile fp
        case err_or of
            Left err
                    | isDoesNotExistError err -> return ()
                    | otherwise  -> throwIO err

            Right _ -> return ()

        where
            fp      = base_dir </> base64UrlFilePath ident

    fssCheckFile (LocalFileStore base_dir _ _) _privacy ident = ioErrorToMonadError $ do
        b <- liftIO $ doesFileExist fp
        if b
            then do
                ident2 <- liftIO $ liftM fileContentIdent $ LB.readFile fp
                $logDebug $ "file already exists with the same checksum"

                unless (ident2 == ident) $ do
                    $logWarn $ fromString $
                        "file exists but checksums mismatch: " <> fp

                return $ ident2 == ident

            else do
                $logDebug $ fromString $ "file does not exists: " <> fp
                return False
        where
            fp      = base_dir </> base64UrlFilePath ident


    fssPublicDownloadUrl (LocalFileStore _ m_mk_pub_url _) = do
        mk_url <- m_mk_pub_url
        return $ \m_mime ident -> liftIO $ mk_url m_mime ident

    fssPrivateDownloadUrl (LocalFileStore _ _ m_mk_pri_url) = do
        mk_url <- m_mk_pri_url
        return $ \e m_mime ident -> liftIO $ mk_url e m_mime ident

    fssDownloadInternal store = Just $ \ident -> ioErrorToMonadError $ do
        downloadFromLocalStore store ident

    fssCopyToPublic (LocalFileStore base_dir _ _) ident = ioErrorToMonadError $ do
        liftIO $ doesFileExist fp
        where
            fp      = base_dir </> base64UrlFilePath ident


downloadFromLocalStore :: (Byteable i, MonadIO m, MonadCatch m) =>
                        LocalFileStore i
                        -> i
                        -> m (Maybe LB.ByteString)
downloadFromLocalStore (LocalFileStore base_dir _ _) ident = do
        let fp      = base_dir </> base64UrlFilePath ident
        err_or <- liftIO $ try $ LB.readFile fp
        case err_or of
            Left err
                    | isDoesNotExistError err -> return Nothing
                    | otherwise  -> throwIO err

            Right x -> return $ Just x

instance
    ( ContentBasedFileIdent i, Byteable i, Eq i
    , MonadIO m, MonadLogger m, MonadCatch m, MonadError String m
    ) =>
    FileStatService m (LocalFileStore i)
    where
    fssFileStat (LocalFileStore base_dir _ _) _privacy ident = ioErrorToMonadError $ do
        err_or_fsize <- liftIO $ try $ fmap (fromIntegral . fileSize) $ getFileStatus fp
        case err_or_fsize of
            Right fsize -> return $ Just $ LocalFileStat fsize
            Left err
                | isDoesNotExistError err -> return Nothing
                | otherwise  -> throwIO err
        where
            fp      = base_dir </> base64UrlFilePath ident

