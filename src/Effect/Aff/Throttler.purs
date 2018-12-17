module Effect.Aff.Throttler
 ( Throttler
 , new
 , run
 , runSimple
 , kill
 ) where

import Prelude

import Data.Foldable (traverse_, for_)
import Data.Maybe (Maybe(..))
import Effect.Aff (Aff)
import Effect.Aff as Aff
import Effect.Aff.AVar (AVar)
import Effect.Aff.AVar as AVar
import Effect.Exception as Exception
import Unsafe.Coerce (unsafeCoerce)


type State output =
  { queuedFiber ∷ Maybe (Aff.Fiber output)
  , currentFiber ∷ Maybe (Aff.Fiber output)
  }

initState ∷ ∀ output . State output
initState =
  { queuedFiber: Nothing
  , currentFiber: Nothing
  }

-- Throttler input output = ∃ setup . ThrottlerF input output setup
type ThrottlerF input output x y =
  { setup ∷ Aff.Fiber input → Aff x
  , operation ∷ Aff.Fiber x → Aff y
  , teardown ∷ Aff.Fiber y → Aff output
  , stateVar ∷ AVar (State output)
  }

foreign import data Throttler ∷ Type → Type → Type

makeThrottler ∷ ∀ input output x y . ThrottlerF input output x y → Throttler input output
makeThrottler = unsafeCoerce

withThrottler ∷ ∀ input output r . (∀ x y . ThrottlerF input output x y → r) → Throttler input output → r
withThrottler f t = f (unsafeCoerce t)

new ∷
  ∀ input output x y .
  (Aff.Fiber input → Aff x) →
  (Aff.Fiber x → Aff y) →
  (Aff.Fiber y → Aff output) →
  Aff (Throttler input output)
new setup operation teardown =
  AVar.new initState <#> \stateVar →
    makeThrottler
      { setup
      , operation
      , teardown
      , stateVar
      }

kill ∷ ∀ input output . Exception.Error → Throttler input output → Aff Unit
kill error = withThrottler \ { stateVar } → do
  state ← AVar.take stateVar
  ado
    AVar.kill error stateVar
    Aff.sequential ado
      Aff.parallel $
        for_ state.queuedFiber \queued -> do
          Aff.killFiber (Exception.error "Throttle killed") queued
          Aff.attempt $ Aff.joinFiber queued
      Aff.parallel $
        for_ state.currentFiber \current -> do
          Aff.killFiber (Exception.error "Throttle killed") current
          Aff.attempt $ Aff.joinFiber current
      in unit
    in unit

modifyAVar ∷ ∀ a . AVar a → (a → a) → Aff Unit
modifyAVar avar f = AVar.take avar >>= \x → AVar.put (f x) avar

run ∷ ∀ input output . Throttler input output → Aff input → Aff (Aff.Fiber output)
run throttler input =
  throttler # withThrottler \ { setup, operation, teardown, stateVar } → do
    { queuedFiber, currentFiber } ← AVar.read stateVar
    case queuedFiber of
      Just queued → pure queued

      Nothing → do
        update ← Aff.suspendAff do
          setupFiber ← Aff.suspendAff do
             setup =<< Aff.forkAff input

          Aff.sequential $
            Aff.parallel (traverse_ (Aff.attempt <<< Aff.joinFiber) currentFiber) <*
            Aff.parallel (Aff.attempt $ Aff.joinFiber setupFiber)

          modifyAVar stateVar \state →
            state
              { queuedFiber = Nothing
              , currentFiber = state.queuedFiber
              }

          operationFiber ← Aff.suspendAff $ operation setupFiber
          _ ← Aff.attempt $ Aff.joinFiber operationFiber
          modifyAVar stateVar _ { currentFiber = Nothing }
          teardown operationFiber

        modifyAVar stateVar _ { queuedFiber = Just update }
        Aff.forkAff $ Aff.joinFiber update

runSimple ∷ ∀ output . Throttler Unit output → Aff (Aff.Fiber output)
runSimple t = run t (pure unit)

