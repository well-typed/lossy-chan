{-# LANGUAGE TypeApplications #-}

module Main (main) where

import Control.Concurrent.LossyChan qualified as LC
import Control.Concurrent.MVar (MVar, newEmptyMVar, putMVar, takeMVar)
import Control.Monad (void)
import Data.Foldable (for_)
import Data.IORef (IORef, mkWeakIORef, newIORef)
import Data.Word (Word16, Word32)
import System.Mem (performMajorGC)
import System.Mem.Weak (Weak)
import System.Timeout (timeout)
import Test.Tasty (defaultMain, testGroup)
import Test.Tasty.HUnit (assertFailure, testCase, (@?=))

main :: IO ()
main =
  defaultMain $
    testGroup "Tests" $
      let n = 4
       in [ testCase "send i 1 >> receive o ==> 1" $ do
              (i, o) <- LC.new @Word32 n
              v <- LC.send i 1 >> LC.receive o
              v @?= 1
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

data TellOnFree = TellOnFree (IORef ()) (Weak (IORef ()))

mkTellOnFree :: MVar () -> IO TellOnFree
mkTellOnFree ping = do
  ref <- newIORef ()
  weakRef <- mkWeakIORef ref (putMVar ping ())
  pure $ TellOnFree ref weakRef

maxLoop :: Word32
maxLoop = fromIntegral (maxBound :: Word16)
