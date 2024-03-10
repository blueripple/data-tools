{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies     #-}
{-# LANGUAGE TypeOperators    #-}

module BlueRipple.Data.FramesUtils
  (
    module BlueRipple.Data.FramesUtils
  )
where

import qualified Control.MapReduce.Core as MapReduce
import qualified Control.MapReduce.Engines as MapReduce

import qualified Control.Foldl as Foldl
import qualified Control.Monad as Monad
import qualified Control.Monad.Primitive as Prim
import qualified Control.Monad.ST as ST

import qualified Data.Hashable as Hashable
import qualified Data.Map.Strict as Map
import qualified Data.Sequence as Seq
import qualified Data.Vector as Vector

import qualified Frames
import qualified Frames.InCore
import qualified Frames.Streamly.InCore as Frames.Streamly
import Frames.Streamly.Streaming.Streamly (StreamlyStream(..))
import qualified Data.Vinyl.TypeLevel as V
import qualified Data.Vinyl as V

import qualified Streamly.Data.Stream as Streamly
import qualified Streamly.Data.StreamK as StreamK
import qualified Streamly.Data.Fold as Streamly.Fold
import qualified Streamly.Internal.Data.Fold as Streamly.Fold
import qualified Streamly.Data.Stream.Prelude as SP

import qualified System.Random.MWC as MWC

import qualified Data.HashTable.Class          as HashTable
import qualified Data.HashTable.ST.Cuckoo      as HashTable.Cuckoo

type Stream = Streamly.Stream
type MonadAsync m = SP.MonadAsync m
sFromFoldable :: (Applicative m, Foldable f) => f a -> Stream m a
sFromFoldable = StreamK.toStream . StreamK.fromFoldable
{-# INLINE sFromFoldable #-}

sMap :: Monad m => (a -> b) -> Stream m a  -> Stream m b
sMap = fmap
{-# INLINE sMap #-}

sConcatEffect :: Monad m => m (Stream m a)  -> Stream m a
sConcatEffect = Streamly.concatEffect

--sClassify :: (Monad m, Ord k) => Streamly.Fold.Fold m a b
{-# INLINE sConcatEffect #-}

frameCompactMR :: ( Ord (Frames.Record ks)
                    , Frames.InCore.RecVec (ks V.++ ds)
                    , Foldable f
                    , Monad m
                    )
  => MapReduce.Unpack (Frames.Record as) (Frames.Record bs)
  -> MapReduce.Assign (Frames.Record ks) (Frames.Record bs) (Frames.Record cs)
  -> Foldl.Fold (Frames.Record cs) (Frames.Record ds)
  -> f (Frames.Record as)
  -> m (Frames.FrameRec (ks V.++ ds))
frameCompactMR unpack (MapReduce.Assign a) fld =
  let unpackS = case unpack of
                  MapReduce.Filter f -> Streamly.filter f
                  MapReduce.Unpack f -> Streamly.concatMap (sFromFoldable . f)
  in  fmap (Frames.toFrame . Map.mapWithKey V.rappend)
      . Streamly.fold (Streamly.Fold.toMap fst $ toStreamlyFold $ Foldl.premap snd fld)
      . sMap a
      . unpackS
      . sFromFoldable
{-# INLINEABLE frameCompactMR #-}


toStreamlyFold :: Monad m => Foldl.Fold a b -> Streamly.Fold.Fold m a b
#if MIN_VERSION_streamly(0,10,0)
toStreamlyFold (Foldl.Fold step init' done) = Streamly.Fold.Fold step' (pure $ Streamly.Fold.Partial $ init') (pure . done) (pure . done) where
  step' x y = pure $ Streamly.Fold.Partial $ step x y
#elif MIN_VERSION_streamly(0,9,0)
toStreamlyFold (Foldl.Fold step init' done) = Streamly.Fold.Fold step' (pure $ Streamly.Fold.Partial $ init') (pure . done) where
  step' x y = pure $ Streamly.Fold.Partial $ step x y
#elif MIN_VERSION_streamly(0,8,0)
toStreamlyFold (Foldl.Fold step init' done) = Streamly.Fold.mkFold step' (Streamly.Fold.Partial init') done where
  step' x y = Streamly.Fold.Partial $ step x y
#else
toStreamlyFold (Foldl.Fold step init done) = Streamly.Fold.mkPure step init done
#endif
{-# INLINE toStreamlyFold #-}

streamGrouper ::
  ( Prim.PrimMonad m
  , Ord (Frames.Record ks)
  , Frames.InCore.RecVec cs
  )
  => Stream m (Frames.Record ks, Frames.Record cs)
  -> Stream m (Frames.Record ks, Frames.FrameRec cs)
streamGrouper = sConcatEffect . Streamly.fold groupFold
  where groupFold = mapToStream <$> Streamly.Fold.toMap fst (Streamly.Fold.lmap snd $ Frames.Streamly.inCoreAoS_F @_ @(StreamlyStream Stream))
        mapToStream = sFromFoldable . Map.toList
{-# INLINEABLE streamGrouper #-}

-- defer toAoS step until grouping is complete?
streamGrouper2 ::
  ( Prim.PrimMonad m
  , Ord (Frames.Record ks)
  , Frames.InCore.RecVec cs
  )
  => Stream m (Frames.Record ks, Frames.Record cs)
  -> Stream m (Frames.Record ks, Frames.FrameRec cs)
streamGrouper2 = sMap (\(!k, (!n, !soa)) ->  (k, Frames.InCore.toAoS n soa))
                 . sConcatEffect
                 . Streamly.fold groupFold
  where groupFold = mapToStream <$> Streamly.Fold.toMap fst (Streamly.Fold.lmap snd $ Frames.Streamly.inCoreSoA_F @_ @(StreamlyStream Stream))
        mapToStream = sFromFoldable . Map.toList
{-# INLINEABLE streamGrouper2 #-}


framesStreamlyMRM ::
  (Prim.PrimMonad m
  , Ord (Frames.Record ks)
  , Frames.InCore.RecVec cs
  , Frames.InCore.RecVec ds
  )
  => MapReduce.MapReduceFoldM m
  (Frames.Record bs)
  (Frames.Record ks)
  (Frames.Record cs)
  Frames.Frame
  (Frames.Record as)
  (Frames.Record ds)
framesStreamlyMRM unpack (MapReduce.AssignM af) reduce =
  let unpackS = case unpack of
        MapReduce.FilterM f -> Streamly.filterM f
        MapReduce.UnpackM f -> Streamly.concatMapM (fmap sFromFoldable . f)
      assignS = Streamly.mapM af
      groupS = streamGrouper
      reduceS = Streamly.mapM (\(!k, !cF) -> reduceFunctionM reduce k cF)
      processS = (Frames.Streamly.inCoreAoS @_ @_ @(StreamlyStream Stream)) . StreamlyStream . reduceS . groupS . assignS . unpackS
  in Foldl.FoldM (\s !a -> return $ a `Streamly.cons` s) (return Streamly.nil) processS
{-# INLINEABLE framesStreamlyMRM #-}

framesStreamlyMR ::
  (Prim.PrimMonad m
  , Ord (Frames.Record ks)
  , Frames.InCore.RecVec cs
  , Frames.InCore.RecVec ds
  )
  => MapReduce.Unpack (Frames.Record as) (Frames.Record bs)
  -> MapReduce.Assign (Frames.Record ks) (Frames.Record bs) (Frames.Record cs)
  -> MapReduce.Reduce (Frames.Record ks) (Frames.Record cs) (Frames.Record ds)
  -> Foldl.FoldM m (Frames.Record as) (Frames.FrameRec ds)
framesStreamlyMR u a r = framesStreamlyMRM
                         (MapReduce.generalizeUnpack u)
                         (MapReduce.generalizeAssign a)
                         (MapReduce.generalizeReduce r)
{-# INLINEABLE framesStreamlyMR #-}


type RecOfVecs m cs = Frames.Record (Frames.InCore.VectorMs (ST.ST (Prim.PrimState m)) cs)
type FeedIn m cs = (Int, Int, RecOfVecs m cs)
type HT m ks cs =  HashTable.Cuckoo.HashTable (Prim.PrimState m) (Frames.Record ks) (FeedIn m cs)

-- TODO: we can assume MonadAsync here since we assume it below
-- so we can use consM or mapM in "done"
classifyHT :: forall m ks cs . (Prim.PrimMonad m
                               , Hashable.Hashable (Frames.Record ks)
--                               , Eq (Frames.Record ks)
                               , Frames.InCore.RecVec cs)
           => Streamly.Fold.Fold m (Frames.Record ks, Frames.Record cs) (Stream m (Frames.Record ks, Frames.FrameRec cs))
#if MIN_VERSION_streamly(0,10,0)
classifyHT = Streamly.Fold.Fold step initF done done where
#elif
classifyHT = Streamly.Fold.Fold step initF done where
#endif
#if MIN_VERSION_streamly(0,8,0)
  initF = Streamly.Fold.Partial <$> Prim.stToPrim HashTable.new

  step ht (!k, !c) = Prim.stToPrim $ do
    HashTable.mutateST ht k (addOneST c)
    return $ Streamly.Fold.Partial ht
#else
  initF :: m (HT m ks cs) = Prim.stToPrim HashTable.new

  step :: HT m ks cs -> (Frames.Record ks, Frames.Record cs) -> m (HT m ks cs)
  step ht (!k, !c) = Prim.stToPrim $ do
    HashTable.mutateST ht k (addOneST c)
    return ht
#endif
  feed (!i, !sz, !mvs') row
    | i == sz = Frames.InCore.growRec (Proxy::Proxy cs) mvs'
                >>= flip feed row . (i, sz*2,)
    | otherwise = do Frames.InCore.writeRec (Proxy::Proxy cs) i mvs' row
                     return (i + 1, sz, mvs')
  initial = do
    mvs <- Frames.InCore.allocRec (Proxy :: Proxy cs) Frames.InCore.initialCapacity
    return (0, Frames.InCore.initialCapacity, mvs)

  addOneST :: Frames.Record cs -> Maybe (FeedIn m cs) -> ST.ST (Prim.PrimState m) (Maybe (FeedIn m cs), ())
  addOneST c vM = fmap (\x -> (Just x, ())) $ case vM of
    Nothing -> initial >>= flip feed c
    Just v -> feed v c

  fin (n, _, mvs') =
    do vs <- Frames.InCore.freezeRec (Proxy::Proxy cs) n mvs'
       return . (n,) $ Frames.InCore.produceRec (Proxy::Proxy cs) vs

  finalize :: (Frames.Record k, FeedIn (ST.ST (Prim.PrimState m)) cs)
           -> (ST.ST (Prim.PrimState m)) (Frames.Record k, Frames.FrameRec cs)
  finalize (!k, !v) = do
    frame <- uncurry Frames.toAoS <$> fin v
    return (k, frame)

  done :: HT m ks cs -> m (Stream m (Frames.Record ks, Frames.FrameRec cs))
  done =  Prim.stToPrim . fmap Streamly.fromList . Monad.join . fmap (traverse finalize) . HashTable.toList
{-
  done :: HT m ks cs -> m (Streamly.SerialT m (Frames.Record ks, Frames.FrameRec cs))
  done = Prim.stToPrim . HashTable.foldM (\s (k, v) -> return $ finalize (k, v) `Streamly.consM` s) Streamly.nil
-}
{-# INLINEABLE streamGrouperHT #-}

streamGrouperHT :: (Prim.PrimMonad m
--                   , Monad m
                   , Hashable.Hashable (Frames.Record ks)
--                   , Eq (Frames.Record ks)
                   , Frames.InCore.RecVec cs
                   )
                => Stream m (Frames.Record ks, Frames.Record cs)
                -> Stream m (Frames.Record ks, Frames.FrameRec cs)
streamGrouperHT = sConcatEffect . Streamly.fold classifyHT


framesStreamlyMRM_HT ::
  (Prim.PrimMonad m
  , Hashable.Hashable (Frames.Record ks)
--  , Eq (Frames.Record ks)
  , Frames.InCore.RecVec cs
  , Frames.InCore.RecVec ds
  )
  => MapReduce.MapReduceFoldM m
  (Frames.Record bs)
  (Frames.Record ks)
  (Frames.Record cs)
  Frames.Frame
  (Frames.Record as)
  (Frames.Record ds)
framesStreamlyMRM_HT unpack (MapReduce.AssignM af) reduce =
  let unpackS = case unpack of
        MapReduce.FilterM f -> Streamly.filterM f
        MapReduce.UnpackM f -> Streamly.concatMapM (fmap sFromFoldable . f)
      assignS = Streamly.mapM af
      groupS = streamGrouperHT
      reduceS = Streamly.mapM (\(!k, !cF) -> reduceFunctionM reduce k cF)
      processS = (Frames.Streamly.inCoreAoS @_ @_ @(StreamlyStream Stream)). StreamlyStream . reduceS . groupS . assignS . unpackS
  in Foldl.FoldM (\s a -> return $ a `Streamly.cons` s) (return Streamly.nil) processS
{-# INLINEABLE framesStreamlyMRM_HT #-}

framesStreamlyMR_HT ::
  (Prim.PrimMonad m
  , Hashable.Hashable (Frames.Record ks)
--  , Eq (Frames.Record ks)
  , Frames.InCore.RecVec cs
  , Frames.InCore.RecVec ds
  )
  => MapReduce.Unpack (Frames.Record as) (Frames.Record bs)
  -> MapReduce.Assign (Frames.Record ks) (Frames.Record bs) (Frames.Record cs)
  -> MapReduce.Reduce (Frames.Record ks) (Frames.Record cs) (Frames.Record ds)
  -> Foldl.FoldM m (Frames.Record as) (Frames.FrameRec ds)
framesStreamlyMR_HT u a r = framesStreamlyMRM_HT
                         (MapReduce.generalizeUnpack u)
                         (MapReduce.generalizeAssign a)
                         (MapReduce.generalizeReduce r)
{-# INLINEABLE framesStreamlyMR_HT #-}


-- | Turn @ReduceM@ into a function we can apply
reduceFunctionM
  :: (Foldable h, Functor h, Monad m) => MapReduce.ReduceM m k x d -> k -> h x -> m d
reduceFunctionM (MapReduce.ReduceM     f) k = f k
reduceFunctionM (MapReduce.ReduceFoldM f) k = Foldl.foldM (f k)
{-# INLINABLE reduceFunctionM #-}


framesStreamlyMRM_SF ::
  (Prim.PrimMonad m
  , Ord (Frames.Record ks)
  , Frames.InCore.RecVec cs
  , Frames.InCore.RecVec ds
  )
  => MapReduce.UnpackM m (Frames.Record as) (Frames.Record bs)
  -> MapReduce.AssignM m (Frames.Record ks) (Frames.Record bs) (Frames.Record cs)
  -> MapReduce.ReduceM m (Frames.Record ks) (Frames.Record cs) (Frames.Record ds)
  -> Frames.FrameRec as
  -> m (Frames.FrameRec ds)
framesStreamlyMRM_SF unpack (MapReduce.AssignM af) reduce =
  let unpackS = case unpack of
        MapReduce.FilterM f -> Streamly.filterM f
        MapReduce.UnpackM f -> Streamly.concatMapM (fmap sFromFoldable . f)
      assignS = Streamly.mapM af
      groupS = streamGrouper2
      reduceS = Streamly.mapM (\(!k, !cF) -> reduceFunctionM reduce k cF)
      processS = reduceS . groupS . assignS . unpackS
  in (Frames.Streamly.inCoreAoS @_ @_ @(StreamlyStream Stream)). StreamlyStream . processS . sFromFoldable
{-# INLINEABLE framesStreamlyMRM_SF #-}


framesStreamlyMR_SF ::
  (Prim.PrimMonad m
  , Ord (Frames.Record ks)
  , Frames.InCore.RecVec cs
  , Frames.InCore.RecVec ds
  )
  => MapReduce.Unpack (Frames.Record as) (Frames.Record bs)
  -> MapReduce.Assign (Frames.Record ks) (Frames.Record bs) (Frames.Record cs)
  -> MapReduce.Reduce (Frames.Record ks) (Frames.Record cs) (Frames.Record ds)
  -> Frames.FrameRec as
  -> m (Frames.FrameRec ds)
framesStreamlyMR_SF u a r = framesStreamlyMRM_SF
                            (MapReduce.generalizeUnpack u)
                            (MapReduce.generalizeAssign a)
                            (MapReduce.generalizeReduce r)
{-# INLINEABLE framesStreamlyMR_SF #-}


fStreamlyMRM_HT ::
  (Prim.PrimMonad m
  , Hashable.Hashable (Frames.Record ks)
  , Frames.InCore.RecVec cs
  , Frames.InCore.RecVec ds
  )
  =>MapReduce.UnpackM m (Frames.Record as) (Frames.Record bs)
  -> MapReduce.AssignM m (Frames.Record ks) (Frames.Record bs) (Frames.Record cs)
  -> MapReduce.ReduceM m (Frames.Record ks) (Frames.Record cs) (Frames.Record ds)
  -> Frames.FrameRec as
  -> m (Frames.FrameRec ds)
fStreamlyMRM_HT unpack (MapReduce.AssignM af) reduce =
  let unpackS = case unpack of
        MapReduce.FilterM f -> Streamly.filterM f
        MapReduce.UnpackM f -> Streamly.concatMapM (fmap sFromFoldable . f)
      assignS = Streamly.mapM af
      groupS = streamGrouperHT
      reduceS = Streamly.mapM (\(!k, !cF) -> reduceFunctionM reduce k cF)
      processS = reduceS . groupS . assignS . unpackS
  in (Frames.Streamly.inCoreAoS  @_ @_ @(StreamlyStream Stream)) . StreamlyStream . processS . sFromFoldable
{-# INLINEABLE fStreamlyMRM_HT #-}

fStreamlyMR_HT ::
  (Prim.PrimMonad m
  , Hashable.Hashable (Frames.Record ks)
--  , Eq (Frames.Record ks)
  , Frames.InCore.RecVec cs
  , Frames.InCore.RecVec ds
  )
  => MapReduce.Unpack (Frames.Record as) (Frames.Record bs)
  -> MapReduce.Assign (Frames.Record ks) (Frames.Record bs) (Frames.Record cs)
  -> MapReduce.Reduce (Frames.Record ks) (Frames.Record cs) (Frames.Record ds)
  -> Frames.FrameRec as
  -> m (Frames.FrameRec ds)
fStreamlyMR_HT u a r = fStreamlyMRM_HT
                       (MapReduce.generalizeUnpack u)
                       (MapReduce.generalizeAssign a)
                       (MapReduce.generalizeReduce r)
{-# INLINEABLE fStreamlyMR_HT #-}

sampleFrame :: (Frames.InCore.RecVec rs, Prim.PrimMonad m) => Word32 -> Int -> Frames.FrameRec rs -> m (Frames.FrameRec rs)
sampleFrame seed n rows = do
  gen <- MWC.initialize (Vector.singleton seed)
  Frames.toFrame <$> sample (Foldl.fold Foldl.list rows) n gen

sample :: Prim.PrimMonad m => [a] -> Int -> MWC.Gen (Prim.PrimState m) -> m [a]
sample ys size = go 0 (l - 1) (Seq.fromList ys) where
    l = length ys
    go !n !i xs g | n >= size = return $! (toList . Seq.drop (l - size)) xs
                  | otherwise = do
                      j <- MWC.uniformR (0, i) g
                      let toI  = xs `Seq.index` j
                          toJ  = xs `Seq.index` i
                          next = (Seq.update i toI . Seq.update j toJ) xs
                      go (n + 1) (i - 1) next g
{-# INLINEABLE sample #-}
