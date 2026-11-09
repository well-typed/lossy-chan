{-# LANGUAGE CPP #-}
{-# LANGUAGE TypeApplications #-}

module Main (main) where

import Control.Concurrent.LossyChan qualified as LC
import Control.Concurrent.MVar (MVar, newEmptyMVar, putMVar, takeMVar)
import Control.Monad (void)
import Data.Foldable (for_)
import Data.IORef (IORef, mkWeakIORef, newIORef)
import Data.Traversable (for)
import Data.Word (Word32, Word8)
import System.Mem (performMajorGC)
import System.Mem.Weak (Weak)
import System.Timeout (timeout)
import Test.Tasty (TestTree, defaultMain, testGroup)
import Test.Tasty.HUnit (assertBool, assertFailure, testCase, (@?=))
import Test.Tasty.QuickCheck (Property, chooseBoundedIntegral, counterexample, forAll, ioProperty, label, testProperty)

main :: IO ()
main = do
  defaultMain $
    testGroup
      "Tests"
      [ unitTests 2
      , unitTests 4
      , propTests
      ]

{- |
Property-based tests.
-}
propTests :: TestTree
propTests =
  testGroup "Prop" $
    [ testProperty "lossless when keeping up" prop_LosslessWhenKeepingUp
    , testProperty "lose 1 when lagging by N+1" prop_Lose1WhenLaggingByNPlus1
    , testProperty "lose N when lagging by 2N+1" prop_LoseNWhenLaggingBy2NPlus1
    ]

{- |
Test that a @lossy-chan@ channel is lossless if the receiver keeps up.
-}
prop_LosslessWhenKeepingUp :: Property
prop_LosslessWhenKeepingUp =
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
  testGroup ("Unit (n=" <> show n <> ")") $
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
    , testCase "for_ [0..n+1] (send i) >> receive o ==> 2" $ do
        (i, o) <- LC.new @Word32 n
        v <- for_ [0 .. n + 1] (LC.send i) >> LC.receive o
        v @?= 2
    , testCase "for_ [0..n*2] (send i) >> receive o ==> n + 1" $ do
        (i, o) <- LC.new @Word32 n
        v <- for_ [0 .. n * 2] (LC.send i) >> LC.receive o
        v @?= n + 1
    , testCase "for_ [0..n*L+1] (send i) >> receive o ==> n * (L - 1) + 1" $ do
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
        p2 <- newEmptyMVar
        t1 <- mkTellOnFree p1
        t2 <- mkTellOnFree p2
        (i, o) <- LC.new n
        LC.send i t1 >> LC.send i t2 >> void (LC.receive o)
        performMajorGC
        timeout 10000 (takeMVar p1)
          >>= maybe (assertFailure "") pure
    ]

{- |
A wrapper around an `IORef` and a `Weak` pointer.

Construct with `mkTellOnFree` to get a value that pings an `MVar` when its finalizer runs.
-}
data TellOnFree = TellOnFree (IORef ()) (Weak (IORef ()))

{- |
Constructor for `TellOnFree`.
-}
mkTellOnFree :: MVar () -> IO TellOnFree
mkTellOnFree ping = do
  ref <- newIORef ()
  weakRef <- mkWeakIORef ref (putMVar ping ())
  pure $ TellOnFree ref weakRef

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
