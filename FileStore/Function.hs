{-# LANGUAGE ScopedTypeVariables #-}
module FileStore.Function where
import qualified Data.ByteString.Lazy       as LB
import Network.Wreq                         as WR
import Network.Mime                         (MimeType)
import Data.Time                            (addUTCTime, NominalDiffTime)
import Control.Lens

import ClassyPrelude
import Control.Monad.Logger
import Control.Monad.Except hiding (mapM_, forM, forM_, mapM)
import Control.Monad.Trans.Maybe

import FileStore.Types


-- | 下载指定 URL 上的文件，其文件的内容副本必须是已知的

-- | 先看能不能直接从远程fetch，不行就直接上传
-- 这个函数的目标是尽量减少从本地上传的带宽
fssFetchRemoteOrUpload ::
    (ContentBasedFileIdent i, Eq i, MonadLogger m
    , FileStoreService m a, FileStoreIdent a ~ i
    ) =>
    a
    -> StorePrivacy
    -> String
    -> m ((LB.ByteString, Maybe MimeType), i)
            -- ^ 负责下载远程文件到本地的操作
            -- fssFetchRemoteOrUpload 会尽量不去使用这个操作
            -- 注意：这个操作是根据前面的url参数生成的（故不帶url参数）
    -> m (Maybe i)
fssFetchRemoteOrUpload store privacy url get_lbs_ident = do
    case fssFetchRemote store of
        Just fetch_func -> do
            -- 支持真正抓取，并自动计算出 file ident
            ident <- fetch_func privacy url
            return $ Just ident

        Nothing -> do
            case fssFetchRemoteSaveAs store of
                Just save_as -> do
                    -- 支持真正抓取，但要调用者提供 file ident
                    (_lbs, ident) <- get_lbs_ident
                    b <- save_as privacy url ident
                    return $ if b then (Just ident) else Nothing

                Nothing -> do
                    -- 完全不支持任何抓取，只能从本地上传
                    ((lbs, m_mime), ident) <- get_lbs_ident
                    ident' <- fssSaveLBS store m_mime privacy lbs
                    return $
                        if ident == ident'
                            then Just ident
                            else Nothing


simpleCachedDownload :: (MonadIO m, ContentBasedFileIdent i) =>
    String -> m (m ((LB.ByteString, Maybe MimeType), i))
simpleCachedDownload url = do
    lbs_ident_ref <- liftIO $ newIORef Nothing
    let get_lbs_ident = liftIO $ do
            m_lbs <- readIORef lbs_ident_ref
            case m_lbs of
                Just x -> return x
                Nothing -> do
                    rb <- WR.get url
                    let lbs = rb ^. responseBody
                        ident = fileContentIdent lbs
                    let m_mime = rb ^? responseHeader "Content-Type"
                    let result = ((lbs, m_mime), ident)
                    writeIORef lbs_ident_ref (Just result)
                    return result

    return $ get_lbs_ident


-- | 逐一用合适的方式（本地上传或远程抓取），把URL指定的文件保存至所有 FileStoreService
saveRemoteFileToFileStores :: forall i m.
    ( MonadIO m, ContentBasedFileIdent i, Eq i
    , MonadLogger m, MonadCatch m, MonadError String m
    ) =>
    [(SomeFileStoreService i m, i -> m (), Text -> m ())]
        -- ^ (SomeFileStoreService, on success, on error)
    -> StorePrivacy
    -> String       -- ^ url
    -> m ()
saveRemoteFileToFileStores stores privacy url = do
    get_lbs_ident :: m ((LB.ByteString, Maybe MimeType), i) <- simpleCachedDownload url
    let handle_one :: i
                    -> (SomeFileStoreService i m, i -> m (), Text -> m ())
                    -> m ()
        handle_one ident (SomeFileStoreService store, on_success, on_error) = do
            case fssFetchRemoteSaveAs store of
                Just save_as -> do
                    -- 支持真正抓取，但要调用者提供 file ident
                    err_or_b <- tryMonadError $ ioErrorToMonadError $ save_as privacy url ident
                    case err_or_b of
                        Left err -> do
                            let msg = fromString $ "IOError when remote fetch: " <> url
                                                    <> ", " <> show err
                            $logError msg
                            on_error msg

                        Right True -> on_success ident

                        Right False -> do
                            let msg = fromString $ "remote fetch failed: " <> url
                            $logError msg
                            on_error msg

                Nothing -> do
                    -- 完全不支持任何抓取，只能从本地上传
                    err_or_lbs_ident <- tryMonadError $ ioErrorToMonadError get_lbs_ident
                    case err_or_lbs_ident of
                        Left err -> do
                            let msg = fromString $ "IOError when download file: " <> url
                                                    <> ", " <> show err
                            $logError msg
                            on_error msg

                        Right ((lbs, m_mime), ident') -> do
                            err_or_ident <- tryMonadError $ ioErrorToMonadError $
                                                fssSaveLBS store m_mime privacy lbs
                            case err_or_ident of
                                Left err -> do
                                    let msg = fromString $ "IOError when saving to file store: " <> url
                                                            <> ", " <> show err
                                    $logError msg
                                    on_error msg

                                Right ident'' -> do
                                    if ident == ident' && ident == ident''
                                        then on_success ident
                                        else do
                                            let msg = fromString $
                                                        "saving to file store returned different ident: "
                                                        <> url
                                            $logError msg
                                            on_error msg

    let start [] = do
                    $logWarn $ "no file store service to use"
                    return ()
        start ((SomeFileStoreService x, on_success, on_error):xs) = do
            err_or_m_ident <- tryMonadError $ ioErrorToMonadError $
                                        fssFetchRemoteOrUpload x privacy url get_lbs_ident
            case err_or_m_ident of
                Left err -> do
                    let msg = fromString $ "IOError when fetching to file store: " <> url
                                            <> ", " <> show err
                    $logError msg
                    on_error msg

                Right Nothing -> do
                    let msg = fromString $
                                "a file store service failed to download: " <> url
                    $logError msg
                    on_error msg
                    start xs

                Right (Just ident) -> do
                    on_success ident
                    mapM_ (handle_one ident) xs

    start stores


-- | 保存本地已持有的文件内容至列表里的所有 FileStoreService
-- 为减少本地上传带宽，会先上传至某个可以提供下载连接的服务商
-- 然后再让其它服务商从前面的服务商处下载
saveLbsToFileStores :: forall i m.
    ( MonadIO m, ContentBasedFileIdent i, Eq i
    , MonadLogger m, MonadCatch m, MonadError String m
    ) =>
    [(SomeFileStoreService i m, i -> m (), Text -> m ())]
        -- ^ (SomeAppFileStoreService, on success, on error)
    -> Maybe MimeType
    -> StorePrivacy
    -> LB.ByteString    -- ^ file content
    -> m ()
saveLbsToFileStores stores m_mime privacy lbs = do
    now <- liftIO getCurrentTime
    let expiry = addUTCTime (fromIntegral $ (60 * 60 *  24 :: Int)) now
    infos :: [((SomeFileStoreService i m, i -> m (), Text -> m ()), IORef (Maybe i), Maybe (Maybe MimeType -> i -> m String), Maybe (String -> i -> m Bool))]
        <- forM stores $ \x@(SomeFileStoreService store, _, _) -> do
        let m_down_url1 = fssPublicDownloadUrl store
            m_down_url2 = fssPrivateDownloadUrl store
        let m_down_url2' = flip fmap m_down_url2 $ \f -> f expiry
        let m_down_url = m_down_url1 <|> m_down_url2'
            m_fetch_func = fmap (\f -> f privacy) $ fssFetchRemoteSaveAs store
        ident_ref <- liftIO $ newIORef Nothing
        return (x, ident_ref, m_down_url, m_fetch_func)
    
    -- 第一轮：只找能提供下载能力的平台
    down_url_ref <- liftIO $ newIORef Nothing
    forM_ infos $
        \((SomeFileStoreService store, on_success, on_error), ident_ref, m_down_url, m_fetch_func) -> do

            let save_ident = on_get_ident store on_success on_error ident_ref
            m_known_down_url <- liftIO $ readIORef down_url_ref
            case (m_known_down_url, m_fetch_func, m_down_url) of
                (Just known_down_url, Just fetch_func, _) -> do
                    -- 已有平台提供下载连接，而且当前平台可以从远程fetch
                    err_or_ident <- tryMonadError $ ioErrorToMonadError $ fetch_func known_down_url right_ident
                    case err_or_ident of
                        Left err -> do
                            let msg = fromString $
                                            "IOError when fetch to file store from: "
                                            <> known_down_url
                                            <> ", " <> show err
                            $logError msg
                            on_error msg

                        Right False -> do
                            let msg = fromString $
                                            "file ident mismatch when fetch to file store from: "
                                            <> known_down_url
                            $logError msg
                            on_error msg

                        Right True -> do
                            liftIO $ writeIORef ident_ref (Just $ right_ident)
                            on_success right_ident


                (Nothing, _, Just mk_down_url) -> do
                    -- 还没有平台能提供下载URL, 而这个平台能提供下载
                    err_or_ident <- tryMonadError $ ioErrorToMonadError $ fssSaveLBS store m_mime privacy lbs
                    case err_or_ident of
                        Left err -> do
                            let msg = fromString $ "IOError when saving to file store"
                                                    <> ": " <> show err
                            $logError msg
                            on_error msg

                        Right ident -> do
                            b <- save_ident ident $ "saving to file store"
                            when b $ do
                                down_url <- mk_down_url m_mime ident
                                liftIO $ writeIORef down_url_ref (Just down_url)

                _ -> return () -- 其它情况，先不处理
        
    -- 第二轮：未完成，且能从远程fetch的平台
    m_known_down_url <- liftIO $ readIORef down_url_ref
    case m_known_down_url of
        Nothing             -> return ()
        Just known_down_url -> do
            forM_ infos $
                \((_store, on_success, on_error), ident_ref, _m_down_url, m_fetch_func) -> do
                    m_ident <- liftIO $ readIORef ident_ref
                    when (isNothing m_ident) $ do
                        case m_fetch_func of
                            Just fetch_func -> do
                                -- 已有平台提供下载连接，而且当前平台可以从远程fetch
                                err_or_b <- tryMonadError $ ioErrorToMonadError $
                                                        fetch_func known_down_url right_ident
                                case err_or_b of
                                    Left err -> do
                                        let msg = fromString $
                                                        "IOError when fetching to file store from: "
                                                        <> known_down_url
                                                        <> ", " <> show err
                                        $logError msg
                                        on_error msg

                                    Right False -> do
                                        let msg = fromString $
                                                        "file ident mismatch when fetch to file store from: "
                                                        <> known_down_url
                                        $logError msg
                                        on_error msg

                                    Right True -> do
                                        liftIO $ writeIORef ident_ref (Just $ right_ident)
                                        on_success right_ident

                            _ -> return ()

    -- 第三轮：处理未完成的平台
    forM_ infos $
        \((SomeFileStoreService store, on_success, on_error), ident_ref, _m_down_url, _m_fetch_func) -> do
            m_ident <- liftIO $ readIORef ident_ref
            case m_ident of
                Nothing -> do
                    ident <- fssSaveLBS store m_mime privacy lbs
                    _ <- on_get_ident store on_success on_error ident_ref ident $ "saving to file store"
                    return ()

                Just _ -> return ()

    where
        right_ident = fileContentIdent lbs

        on_get_ident ::
            (FileStoreIdent a ~ i, FileStoreService m a) =>
            a
            -> (i -> m ())
            -> (Text -> m())
            -> IORef (Maybe i)
            -> i
            -> String
            -> m Bool
        on_get_ident store on_success on_error ident_ref ident op_str = do
            if ident == right_ident
                then do
                    liftIO $ writeIORef ident_ref (Just ident)
                    on_success ident
                    return True
                else do
                    let msg = fromString $
                                    "file ident mismatch when " <> op_str <> "."
                    $logError msg
                    fssDelete store privacy ident
                    on_error msg
                    return False

-- | 逐一尝试所有 File Store Service
-- 直至第一个能提供所指定文件的内容
downloadFromAnyFileStore :: ( MonadIO m, Functor m, Eq i, Functor f
                            , HasFileStoreService i m a, Foldable f
                            ) =>
                            f a
                            -> i
                            -> m (Maybe LB.ByteString)
downloadFromAnyFileStore stores ident = do
    -- 这是一批可以直接下载到文件的函数
    runMaybeT $ asum $ flip fmap stores $
                            \fs ->
                                let store = getFileStoreService fs
                                in case fssDownloadInternal store of
                                    Nothing -> mzero
                                    Just g  -> MaybeT $ g ident

mimeFromAnyFileStore :: ( MonadIO m, Functor m, Eq i, Functor f
                            , HasFileStoreService i m a, Foldable f
                            ) =>
                            f a
                            -> i
                            -> m (Maybe MimeType)
mimeFromAnyFileStore stores ident = do
    liftM join $
        runMaybeT $ asum $ flip fmap stores $
                            \fs ->
                                case getFileStoreService fs of
                                    SomeFileStoreService store ->
                                        liftM getMimeType $
                                            asum $ map (\p -> MaybeT $ fssFileStat store p ident)
                                                    [minBound..maxBound]

data DownloadUrlOption = UsePublicUrl | UsePrivateUrl | CheckExistence
                        deriving (Eq, Ord, Enum, Bounded)

-- | 逐一尝试所有 File Store Service，找出能提供下载URL
mkDownloadUrlFromAnyFileStore ::
    ( MonadIO m, Functor m, Eq i, Traversable f , HasFileStoreService i m a, Foldable f) =>
    [DownloadUrlOption]
    -> f a
    -> NominalDiffTime
    -> Maybe MimeType
    -> i
    -> m (Maybe String)
mkDownloadUrlFromAnyFileStore opts stores ttl m_mime ident = do
    now <- liftIO getCurrentTime
    let url_expiry = addUTCTime ttl now
    func_list <- forM stores $ \fs -> do
        let store = getFileStoreService fs
            down_pub = do
                    b <- if check_exists
                            then fssCheckFile store StorePublic ident
                            else return True
                    return $ do
                        guard b
                        MaybeT $ mapM (\f -> f m_mime ident) $ fssPublicDownloadUrl store

            down_pri = do
                    b <- if check_exists
                            then fssCheckFile store StorePrivate ident
                            else return True
                    return $ do
                        guard b
                        MaybeT $ mapM (\f -> f url_expiry m_mime ident) $ fssPrivateDownloadUrl store

        liftM asum $ sequence $
                    [ if use_private_url then down_pri else return mzero
                        -- prefer private url when possible
                    , if use_public_url then down_pub else return mzero
                    ]

    runMaybeT $ asum $ func_list
    where
        use_public_url = UsePublicUrl `elem` opts
        use_private_url = UsePrivateUrl `elem` opts
        check_exists = CheckExistence `elem` opts
