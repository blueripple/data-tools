{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE UndecidableInstances #-}

module BlueRipple.Data.KeyedTables
  (
    module BlueRipple.Data.KeyedTables
  )
where

import qualified BlueRipple.Data.Keyed as BRK
import qualified Control.Foldl as FL
import qualified Data.Array as Array
import qualified Data.Map.Strict as Map
import qualified Data.Set as Set
import qualified Data.Csv as CSV
import qualified Data.Vector as Vec
import qualified Say

{-
newtype TableKey a = TableKey { unTableKey :: a }

data Table k a where
  Table :: Array.Ix (TableKey k) => Array.Array (TableKey k) a -> Table k a

deriving stock Functor (Table k)
deriving stock Foldable (Table k)

data Partition a b where
  Partition :: (BRK.FiniteSet a, BRK.FiniteSet b) => (b -> Set a) -> Partition a b
-}
-- This will error if a has no elements
{-
rekeyArray :: forall k1 k2 x.(Array.Ix k1, Array.Ix k2, BRK.FiniteSet k2, Monoid x)
      => (k2 -> Set.Set k1) -> Array.Array k1 x -> Array.Array k2 x
rekeyArray f t =
  let af :: BRK.AggF Bool k2 k1
      af = BRK.AggF (flip Set.member . f)
      rekeyF = BRK.aggFoldAll af (BRK.dataFoldCollapseBool FL.mconcat)
  in Array.array BRK.finiteSetMinMax $ FL.fold rekeyF $ Array.assocs t


rekeyMap :: forall k1 k2 x.(Ord k1, Ord k2, BRK.FiniteSet k2, Num x)
      => (k2 -> Set.Set k1) -> Map k1 x -> Map k2 x
rekeyMap f t =
  let af :: BRK.AggF Bool k2 k1
      af = BRK.AggF (flip Set.member . f)
      rekeyF = BRK.aggFoldAll af (BRK.dataFoldCollapseBool FL.sum)
  in Map.fromList $ FL.fold rekeyF $ Map.toList t
-}

reKeyMap :: forall k1 k2 x . (Ord k2, BRK.FiniteSet k2, Num x)
      => (k2 -> k1 -> Bool) -> Map k1 x -> Map k2 x
reKeyMap f t =
  let af :: BRK.AggF Bool k2 k1
      af = BRK.AggF f
      rekeyF = BRK.aggFoldAll af (BRK.dataFoldCollapseBool FL.sum)
  in Map.fromList $ FL.fold rekeyF $ Map.toList t

keyF :: Eq k1 => (k2 -> [k1]) -> k2 -> k1  -> Bool
keyF f k2 k1 = k1 `elem` f k2

{-
unNest :: (Array.Ix k1, Array.Ix k2, BRK.FiniteSet k1, BRK.FiniteSet k2)
       => (Array.Array k1 (Array.Array k2 x)) -> Array.Array (k1, k2) x
unNest (Table t) =
  let rekeyOne :: k1 -> (k2, x) -> ((k1, k2), x)
      rekeyOne k1 (k2, x) = ((k1, k2), x)
      rekey :: (k1, [(k2, x)]) -> [(k1, k2), x]
      rekey (k1, xs) = fmap (rekeyOne k1) xs
  in Array.array finiteSetMinMax $ fmap (rekey . second Array.assocs) $ Array.assocs t
-}
-- What we want is to parse a csv file which contains some given prefix cols and then census labeled counts
-- and produce a (long) frame with the data from that table
-- Better, the table part should be a fold so we can compose this for multiple tables.

-- b is some key unique to each set of columns in a table
-- And we can map it to the typed description of the table, for later.
tableDescriptions :: Ord b => (b -> Map c Text) -> [b] -> Map b [Text]
tableDescriptions describe bs = Map.fromList $ fmap (\b -> (b, Map.elems $ describe b)) bs

allTableDescriptions :: (Ord b, BRK.FiniteSet d) => (b -> Map c Text) -> (d -> [b]) -> Map b [Text]
allTableDescriptions describe fromType = Map.unions $ (tableDescriptions describe . fromType) <$> Set.toList BRK.elements

data TableRow a c = TableRow { prefix :: a, counts :: c}
deriving stock instance (Show a, Show c) => Show (TableRow a c)
deriving stock instance Functor (TableRow a)

type RawTablesRow a b = TableRow a (Map b (Map Text Int))

parseTablesRow :: CSV.FromNamedRecord a  => Map b [Text] -> CSV.NamedRecord -> CSV.Parser (RawTablesRow a b)
parseTablesRow tableHeadersByName r = TableRow <$> CSV.parseNamedRecord r <*> traverse (parseTable r) tableHeadersByName where
  lookupOne :: CSV.NamedRecord -> Text -> CSV.Parser (Text, Int)
  lookupOne r' t = fmap (t,) $ CSV.lookup r' $ encodeUtf8 t
  parseTable :: CSV.NamedRecord -> [Text] -> CSV.Parser (Map Text Int)
  parseTable r' headers = Map.fromList <$> traverse (lookupOne r') headers

decodeCSVTables :: forall a b.(CSV.FromNamedRecord a)
                => Map b [Text]
                -> LByteString
                -> Either Text (CSV.Header, Vec.Vector (RawTablesRow a b))
decodeCSVTables tableHeaders =
  first toText . CSV.decodeByNameWithP (parseTablesRow tableHeaders) CSV.defaultDecodeOptions

decodeCSVTablesFromFile :: forall a b.(CSV.FromNamedRecord a)
                        => Map b [Text]
                        -> FilePath
                        -> IO (Either Text (CSV.Header, Vec.Vector (RawTablesRow a b)))
decodeCSVTablesFromFile tableHeaders fp = Say.say ("decodeTablesFromCSV: loading from " <> toText fp) >> decodeCSVTables tableHeaders <$> readFileLBS fp

typeOneTable' :: (Ord b, Show b, Show a, Show c) => (b -> Map c Text) -> RawTablesRow a b -> b -> Either Text (Map c Int)
typeOneTable' tableDescription rtr@(TableRow _ cm) tableKey = do
  countMap <- maybeToRight ("Failed to find \"" <> show tableKey <> " in TableRow: " <> show rtr) $ Map.lookup tableKey cm
  let description = tableDescription tableKey
      typedMap = mapCompose countMap description
  if Map.size typedMap /= Map.size description
    then Left $ "Mismatch when composing maps for tableKey=" <> show tableKey <> "; counts=" <> show countMap <> "; description=" <> show description
    else Right typedMap

typeOneTable :: (Ord b, Show b, Show a, Show c) => (b -> Map c Text) -> RawTablesRow a b -> b -> Either Text (TableRow a (Map c Int))
typeOneTable tableDescription rtr@(TableRow p _cm) tableKey = fmap (TableRow p) $ typeOneTable' tableDescription rtr tableKey

typeAndMergeTables :: (Ord b, Array.Ix c, Show b, Show a, Show c)
                   => (b -> Map c Text) -> [b] -> RawTablesRow a b -> Either Text (TableRow a (Map c Int))
typeAndMergeTables tableDescription tableKeys rtr@(TableRow p _) =
  TableRow p . Map.unionsWith (+) <$> traverse (typeOneTable' tableDescription rtr) tableKeys

consolidateTables ::  forall d a b c.
                      (Ord b
                      , Array.Ix c
                      , Show b
                      , Show a
                      , Show c
                      , Ord d
                      , BRK.FiniteSet d
                      ) => (b -> Map c Text) -> (d -> [b]) -> RawTablesRow a b -> Either Text (TableRow a (Map (d, c) Int))
consolidateTables tableDescription keysFrom rtr@(TableRow p _) = do
  let allD :: [d] = Set.toList BRK.elements
      doOne d = fmap (d,) $ counts <$> typeAndMergeTables tableDescription (keysFrom d) rtr
      remap (d, m) = Map.fromAscList $ fmap (\(c, x) -> ((d, c), x)) $ Map.toAscList m
  merged <- traverse doOne allD
  let remapped = remap <$> merged
  return $ TableRow p $ Map.unions remapped

reKeyTable :: (Ord k2, BRK.FiniteSet k2, Num x) => (k2 -> k1 -> Bool) -> TableRow a (Map k1 x) -> TableRow a (Map k2 x)
reKeyTable f = fmap (reKeyMap f)

-- This is present in containers >= 0.6.3.1 but that has conflicts.  Fix eventually
mapCompose  :: Ord b => Map b c -> Map a b -> Map a c
mapCompose bc !ab
  | Map.null bc = Map.empty
  | otherwise = Map.mapMaybe (bc Map.!?) ab
