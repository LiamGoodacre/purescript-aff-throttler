module Test.Main where

import Prelude

import Data.Foldable (traverse_)
import Data.Time.Duration (Milliseconds(..))
import Data.Unfoldable (replicateA)
import Effect (Effect)
import Effect.Aff (Aff)
import Effect.Aff as Aff
import Effect.Aff.AVar (AVar)
import Effect.Aff.AVar as AVar
import Effect.Aff.Throttler as Throttler
import Effect.Class (liftEffect)
import Effect.Console (log)
import Effect.Exception as Exception

getInc ∷ AVar Int → Aff Int
getInc var = do
  id ← AVar.take var
  AVar.put (id + 1) var
  pure id

main ∷ Effect Unit
main = Aff.launchAff_ do
  let
    setup getId = do
      id ← Aff.joinFiber getId
      liftEffect (log $ "Wait " <> show id)
      Aff.delay (Milliseconds 100.0)
      pure id

    operation getId = do
      id ← Aff.joinFiber getId
      liftEffect (log $ "Work " <> show id)
      Aff.delay (Milliseconds 50.0)
      pure id

    teardown getId = do
      id ← Aff.joinFiber getId
      liftEffect (log $ "Done " <> show id)

  throttledSubmit ← Throttler.new setup operation teardown

  counterVar1 ← AVar.new 0
  counterVar2 ← AVar.new 0

  let launchSubmits ∷ Aff (Array (Aff.Fiber Unit))
      launchSubmits = do
        Aff.delay (Milliseconds 50.0)
        replicateA 20000 do
          attemptId ← getInc counterVar1
          Throttler.run throttledSubmit do
            actualId ← getInc counterVar2
            pure { attemptId, actualId }

  traverse_ Aff.joinFiber =<<
    launchSubmits <> launchSubmits <> launchSubmits <> launchSubmits

  Throttler.kill (Exception.error "Killed") throttledSubmit
  AVar.kill (Exception.error "Killed") counterVar1
  AVar.kill (Exception.error "Killed") counterVar2

  liftEffect $ log "End"

