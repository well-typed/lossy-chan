{-# LANGUAGE CPP #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TypeApplications #-}

module Main where

import Control.Concurrent.LossyChan qualified as LC
import Control.Concurrent.MVar (MVar, newEmptyMVar, putMVar, takeMVar)
import Control.Exception (ArrayException (IndexOutOfBounds), assert, handle, throw)
import Control.Monad (replicateM, unless, void)
import Data.Foldable (for_)
import Data.IORef (IORef, mkWeakIORef, modifyIORef', newIORef, readIORef)
import Data.List (unsnoc)
import Data.Traversable (for)
import Data.Word (Word32, Word8)
import GHC.Stack (HasCallStack)
import System.Mem (performBlockingMajorGC, performMajorGC)
import System.Mem.Weak (Weak)
import System.Timeout (timeout)
import Test.Tasty (TestTree, defaultMain, testGroup)
import Test.Tasty.HUnit (assertBool, assertFailure, testCase, (@?=))
import Test.Tasty.QuickCheck (Arbitrary (..), Gen, Property, chooseBoundedIntegral, counterexample, forAll, ioProperty, label, oneof, testProperty)

main :: IO ()
main = do
  defaultMain $
    testGroup
      "Tests"
      [ unitTests 2
      , unitTests 8
      , unitTests 32
      , unitTests 128
      , propTests
      ]

{- |
Property-based tests.
-}
propTests :: TestTree
propTests =
  testGroup "Prop" $
    [ testProperty "lossless when keeping up with alternating sender and receiver" prop_LosslessWhenKeepingUpSimp
    , testProperty "lossless when keeping up with arbitrary trace" prop_LosslessWhenKeepingUpArb
    , testProperty "lose 1 when lagging by N+1" prop_Lose1WhenLaggingByNPlus1
    , testProperty "lose N when lagging by 2N+1" prop_LoseNWhenLaggingBy2NPlus1
    ]

{- |
Test that a @lossy-chan@ channel is lossless if the receiver keeps up.

This test uses a single sender and a single receiver that alternate sending and receiving.
-}
prop_LosslessWhenKeepingUpSimp :: Property
prop_LosslessWhenKeepingUpSimp =
  forAll (chooseBoundedIntegral (1, 128)) $ \n ->
    forAll (chooseBoundedIntegral (1, 2 * n * maxLoop)) $ \m ->
      label (if n >= m then "N >= M" else "N <  M") $
        ioProperty $ do
          (i, o) <- LC.new @Word32 n
          let xs = [0 .. m]
          and
            <$> ( for xs $ \x -> do
                    LC.send i x
                    mby <- LC.tryReceive o
                    pure (mby == Just x)
                )

{- |
Test that a @lossy-chan@ channel is lossless if all receivers keep up.

This test uses an arbitrary trace.
-}
prop_LosslessWhenKeepingUpArb :: TraceWithConfig -> Property
prop_LosslessWhenKeepingUpArb twc@(MkTraceWithConfig MkTraceConfig{..} _) =
  label ("with " <> nLabel <> ", " <> mLabel <> ", " <> oknLabel <> ", " <> olnLabel) $
    ioProperty (runTrace twc)
 where
  nLabel :: String
  nLabel
    | n >= 128 = "N >= 128"
    | n >= 64 = "N >= 64 "
    | n >= 32 = "N >= 32 "
    | n >= 16 = "N >= 16 "
    | n >= 8 = "N >= 8  "
    | n >= 4 = "N >= 4  "
    | n >= 2 = "N >= 2  "
    | otherwise = "N == 1  "
  mLabel :: String
  mLabel
    | m >= n * maxLoop = "M >= N * maxLoop"
    | otherwise = "M <  N * maxLoop"
  oknLabel :: String
  oknLabel
    | okn >= 2 = "multiple timely receivers"
    | otherwise = "one timely receiver"
  olnLabel :: String
  olnLabel
    | oln >= 2 = "multiple lagging receivers"
    | oln == 1 = "one lagging receiver"
    | otherwise = "no lagging receivers"

{- |
Test that after N+1 sends the first value received is the second value sent.
-}
prop_Lose1WhenLaggingByNPlus1 :: Property
prop_Lose1WhenLaggingByNPlus1 =
  forAll (chooseBoundedIntegral (1, 128)) $ \n ->
    ioProperty $ do
      (i, o) <- LC.new @Word32 n
      let xs = [0 .. n]
      for_ xs $ (LC.send i)
      mby <- LC.tryReceive o
      pure $
        counterexample ("Expected 1, but received " <> show mby) $
          mby == Just 1

{- |
Test that after N+1 sends the first value received is the second value sent.
-}
prop_LoseNWhenLaggingBy2NPlus1 :: Property
prop_LoseNWhenLaggingBy2NPlus1 =
  forAll (chooseBoundedIntegral (1, 128)) $ \n ->
    ioProperty $ do
      (i, o) <- LC.new @Word32 n
      let xs = [0 .. 2 * n]
      for_ xs $ (LC.send i)
      mby <- LC.tryReceive o
      pure $
        counterexample ("Expected " <> show (n + 1) <> ", but received " <> show mby) $
          mby == Just (n + 1)

{- |
Unit tests.
-}
unitTests :: Word32 -> TestTree
unitTests n =
  testGroup ("Unit (with N = " <> show n <> ")") $
    [ testCase "send i 1 >> receive o ==> 1" $ do
        (i, o) <- LC.new @Word32 n
        v <- LC.send i 1 >> LC.receive o
        v @?= 1
    , testCase "for [1,2] (\\x -> send i x >> receive 0) ==> [1,2]" $ do
        (i, o) <- LC.new @Int n
        x1 <- LC.send i 1 >> LC.receive o
        x1 @?= 1
        x2 <- LC.send i 2 >> LC.receive o
        x2 @?= 2
    , testCase "send i 1 >> receive o >> receive o ==> block" $ do
        (i, o) <- LC.new @Word32 n
        v <- LC.send i 1 >> LC.receive o
        v @?= 1
        w <- timeout 1000 (LC.receive o)
        w @?= Nothing
    , testCase "for_ [0..N+1] (send i) >> receive o ==> 2" $ do
        (i, o) <- LC.new @Word32 n
        v <- for_ [0 .. n + 1] (LC.send i) >> LC.receive o
        v @?= 2
    , testCase "for_ [0..N*2] (send i) >> receive o ==> N + 1" $ do
        (i, o) <- LC.new @Word32 n
        v <- for_ [0 .. n * 2] (LC.send i) >> LC.receive o
        v @?= n + 1
    , testCase "for_ [0..N*L+1] (send i) >> receive o ==> N * (L - 1) + 1" $ do
        assertBool "Library was not built with --flags=+small-loop-counter." smallLoopCounterEnabled
        (i, o) <- LC.new @Word32 n
        v <- for_ [0 .. n * maxLoop] (LC.send i) >> LC.receive o
        v @?= n * (maxLoop - 1) + 1
    , testCase "send i 1 >> traverse receive [o1,o2] ==> [1,1]" $ do
        (i, o1) <- LC.new @Word32 n
        o2 <- LC.dup o1
        vs <- LC.send i 1 >> traverse LC.receive [o1, o2]
        vs @?= [1, 1]
    , testCase "early free" $ do
        p1 <- newEmptyMVar
        t1 <- mkTattleTale p1
        (i, o) <- LC.new n
        LC.send i (Left t1)
        LC.send i (Right ())
        void (LC.receive o)
        performBlockingMajorGC
        timeout 100000 (takeMVar p1)
          >>= maybe (assertFailure "t1 was not freed") pure
    , testCase "receive hangs if InChan is dropped" $ do
        let handleNoChan LC.ErrNoInChan = pure $ Left LC.ErrNoInChan
        (_, o) <- LC.new @() n
        performMajorGC
        v <- handle handleNoChan (Right <$> timeout 10000 (LC.receive o))
        case v of
          Left LC.ErrNoInChan -> pure ()
          Right _ -> assertFailure "ErrNoInChan exception not thrown."
    ]

--------------------------------------------------------------------------------
-- Maximum Loop Count
--------------------------------------------------------------------------------

{- |
The maximum loop value for a @ChanPos@.

This value is the `maxBound` of `Word8`. In the actual implementation,
`maxLoop` would be `maxBound` of `Word32`. To test the overflow cases,
build the library with @--flags=+small-loop-counter@.
-}
maxLoop :: Word32
maxLoop = fromIntegral (maxBound :: Word8)

{- |
Whether or not the library was built with @--flags=+small-loop-counter@.
-}
smallLoopCounterEnabled :: Bool
#ifdef LOSSY_CHAN_SMALL_LOOP_COUNTER
smallLoopCounterEnabled = True
#else
smallLoopCounterEnabled = False
#endif

--------------------------------------------------------------------------------
-- TattleTale
--------------------------------------------------------------------------------

{- |
A wrapper around an `IORef` and a `Weak` pointer.

Construct with `mkTattleTale` to get a value that pings an `MVar` when its finalizer runs.
-}
data TattleTale = TattleTale (IORef ()) (Weak (IORef ()))

{- |
Constructor for `TattleTale`.
-}
mkTattleTale :: MVar () -> IO TattleTale
mkTattleTale ping = do
  ref <- newIORef ()
  weakRef <- mkWeakIORef ref (putMVar ping ())
  pure $ TattleTale ref weakRef

--------------------------------------------------------------------------------
-- Arbitrary Traces
--------------------------------------------------------------------------------

{- |
This type holds a `Trace` with the `TraceConfig` used for its generation.
-}
data TraceWithConfig = MkTraceWithConfig TraceConfig Trace
  deriving (Show)

instance Arbitrary TraceWithConfig where
  arbitrary = do
    tc <- genTraceConfig
    t <- genTrace tc
    pure $ MkTraceWithConfig tc t

  shrink (MkTraceWithConfig tc t) = do
    tc' <- shrinkTraceConfig tc
    let t' = shrinkTrace tc' t
    pure $ MkTraceWithConfig tc' t'

{- |
This type holds the configuration for trace generation.
-}
data TraceConfig = MkTraceConfig
  { n :: !Word32
  -- ^ The size of the channel ring buffer.
  , m :: !Word32
  -- ^ The total number of messages to send.
  , okn :: !Int
  -- ^ The total number of receivers that must keep up.
  , oln :: !Int
  -- ^ The total number of receivers that may lag behind.
  }
  deriving (Show)

{- |
This type represents the operations on a @lossy-chan@ channel.
-}
data TraceOp
  = -- | @`Send` x t@ sends @x@ over the `LC.InChan`.
    Send !Word32
  | -- | @`ReceiveK` k x t@ receives @x@ over the @k@'th `LC.OutChan` that must keep up.
    ReceiveK !Int !Word32
  | -- | @`Receive` l t@ receives some value over the @l@'th `LC.OutChan` that may lag behind.
    ReceiveL !Int
  deriving (Show)

infixr 1 :>

{- |
This type represents a trace of the usage of a @lossy-chan@ channel.
-}
data Trace
  = TraceOp :> Trace
  | End

instance Show Trace where
  showsPrec p (to :> t) = showParen (p > 1) $ showsPrec 1 to . showString " :> " . showsPrec 1 t
  showsPrec _ End = showString "End"

{- |
This type holds the state of the system after at some point in a `Trace`.
-}
data TraceState = MkTraceState
  { ic :: !Word32
  -- ^ Number of messages sent over `LC.InChan`.
  , okcs :: ![Word32]
  -- ^ Number of messages received per `LC.OutChan` that must keep up.
  , olcs :: ![Word32]
  -- ^ Number of messages received per `LC.OutChan` that may lag behind.
  }
  deriving (Show)

{- |
The initial system state. No messages have been sent or received.
-}
initTraceState :: TraceConfig -> TraceState
initTraceState MkTraceConfig{..} =
  MkTraceState{ic = 0, okcs = replicate okn 0, olcs = replicate oln 0, ..}

{- |
Run the given trace.
-}
runTrace :: TraceWithConfig -> IO ()
runTrace (MkTraceWithConfig MkTraceConfig{..} t0) = do
  (i, o) <- LC.new n
  oks <- (o :) <$> replicateM (okn - 1) (LC.dup o)
  ols <- replicateM oln (LC.dup o)
  olxs <- newIORef (replicate oln Nothing)
  let go :: Trace -> IO ()
      go End = pure ()
      go (Send x :> t) = do
        LC.send i x
        go t
      go (ReceiveK k x :> t) = do
        let ok = oks !! k
        y <- LC.receive ok
        unless (x == y) $ error $ "Expected " <> show x <> ", but received " <> show y
        go t
      go (ReceiveL l :> t) = do
        let ol = ols !! l
        mbx <- (!! l) <$> readIORef olxs
        mby <- handle (\LC.ErrNoInChan -> pure Nothing) (LC.tryReceive ol)
        case (mbx, mby) of
          (Just x, Just y)
            | x == y -> error $ "Received duplicate message " <> show x
            | x > y -> error $ "Received " <> show x <> " before " <> show y
          _ -> pure ()
        modifyIORef' olxs ((const mby) `at` l)
        go t
  go t0

--------------------------------------------------------------------------------
-- Generation

{- |
Generate an arbitrary `TraceConfig`.
-}
genTraceConfig :: Gen TraceConfig
genTraceConfig = do
  n <- chooseBoundedIntegral (1, 128)
  m <- chooseBoundedIntegral (2 * n, 2 * n * maxLoop)
  okn <- chooseBoundedIntegral (2, 4)
  oln <- chooseBoundedIntegral (0, 2)
  pure MkTraceConfig{..}

{- |
Generate an arbitrary `Trace`.
-}
genTrace :: TraceConfig -> Gen Trace
genTrace tc =
  go (id, initTraceState tc)
 where
  go :: (Trace -> Trace, TraceState) -> Gen Trace
  go (tf, ts) = case step tc ts of
    [] -> assert (final tc ts) $ pure (tf End)
    op_ts's -> oneof [go (tf . (op :>), ts') | (op, ts') <- op_ts's]

{- |
List all possible traces for the `TraceConfig`.
-}
allTraces :: TraceConfig -> [Trace]
allTraces tc =
  go (id, initTraceState tc)
 where
  go :: (Trace -> Trace, TraceState) -> [Trace]
  go (tf, ts) = case step tc ts of
    [] -> assert (final tc ts) [tf End]
    op_ts's -> [t | (op, ts') <- op_ts's, t <- go (tf . (op :>), ts')]

{- |
Apply a `TraceOp` to the `TraceState`.
-}
tr :: TraceState -> TraceOp -> TraceState
tr MkTraceState{..} = \case
  Send _ -> MkTraceState{ic = ic + 1, ..}
  ReceiveK k _ -> MkTraceState{okcs = ((+ 1) `at` k) okcs, ..}
  ReceiveL l -> MkTraceState{olcs = ((+ 1) `at` l) olcs, ..}

{- |
Generate a list of valid `TraceOp`.
-}
step :: TraceConfig -> TraceState -> [(TraceOp, TraceState)]
step tc ts =
  assert (not (null op_ts's) || final tc ts) op_ts's
 where
  op_ts's = [(op, ts') | op <- ops tc ts, let ts' = tr ts op, valid tc ts']

{- |
Generate a list of possible `TraceOp`.
-}
ops :: TraceConfig -> TraceState -> [TraceOp]
ops MkTraceConfig{..} MkTraceState{..} =
  concat
    [ [Send ic]
    , [ReceiveK k (okcs !! k) | k <- [0 .. okn - 1]]
    , [ReceiveL l | l <- [0 .. oln - 1]]
    ]

final :: TraceConfig -> TraceState -> Bool
final MkTraceConfig{..} MkTraceState{..} =
  ic == m && all (\okc -> okc == m) okcs && all (\olc -> olc <= m) olcs

valid :: TraceConfig -> TraceState -> Bool
valid MkTraceConfig{..} MkTraceState{..} =
  and
    [ -- At most m messages are sent.
      ic <= m
    , -- The difference between the number of sent and the number of received
      -- messages by channels that must keep up is at least 0 and at most n.
      all (\okc -> let dm = asInt ic - asInt okc in 0 <= dm && dm <= asInt n) okcs
    , -- The difference between the number of sent and the number of received
      -- messages by channels that may lag behind is at least 0.
      all (\olc -> let dm = asInt ic - asInt olc in 0 <= dm) olcs
    ]
 where
  asInt :: Word32 -> Int
  asInt = fromIntegral

{- |
Apply the given function at the given index in the given list.
-}
at :: (HasCallStack) => (a -> a) -> Int -> [a] -> [a]
(_ `at` k) [] = throw (IndexOutOfBounds $ show k)
(f `at` 0) (x : xs) = f x : xs
(f `at` k) (x : xs) = x : (f `at` (k - 1)) xs

--------------------------------------------------------------------------------
-- Shrinking

{- |
Shrink a `TraceConfig`.
-}
shrinkTraceConfig :: TraceConfig -> [TraceConfig]
shrinkTraceConfig MkTraceConfig{..}
  | null tcs = []
  | otherwise = init tcs
 where
  tcs = do
    n' <- [1 .. n]
    m' <- [1 .. m]
    okn' <- [1 .. okn]
    oln' <- [0 .. oln]
    pure $ MkTraceConfig n' m' okn' oln'

{- |
Shrink a `Trace` to match the new `TraceConfig`.

__Warning:__
This function assumes that the given `TraceConfig` is smaller than the
`TraceConfig` that was used to generate the trace.
-}
shrinkTrace :: TraceConfig -> Trace -> Trace
shrinkTrace tc = go (initTraceState tc)
 where
  go :: TraceState -> Trace -> Trace
  go _ts End = End
  go ts (op :> t)
    | inBounds tc op, let ts' = tr ts op, valid tc ts = op :> go ts' t
    | otherwise = go ts t

{- |
Check whether a `TraceOp` is in bounds.
-}
inBounds :: TraceConfig -> TraceOp -> Bool
inBounds MkTraceConfig{..} = \case
  Send _ -> True
  ReceiveK k _ -> k < okn
  ReceiveL l -> l < oln
