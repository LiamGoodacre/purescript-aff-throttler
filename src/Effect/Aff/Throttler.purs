module Effect.Aff.Throttler
 ( Throttler
 , new
 , run
 , runSimple
 , kill
 ) where

import Prelude

import Data.Foldable (traverse_)
import Data.Maybe (Maybe(..))
import Effect.Aff (Aff)
import Effect.Aff as Aff
import Effect.Aff.AVar (AVar)
import Effect.Aff.AVar as AVar
import Effect.Exception as Exception
import Unsafe.Coerce (unsafeCoerce)


type State =
  { queuedFiber ∷ Maybe (Aff.Fiber Unit)
  , currentFiber ∷ Maybe (Aff.Fiber Unit)
  }

initState ∷ State
initState =
  { queuedFiber: Nothing
  , currentFiber: Nothing
  }

-- Throttler input = ∃ b c . ThrottlerF input b c
type ThrottlerF input b c =
  { setup ∷ Aff.Fiber input → Aff b
  , operation ∷ Aff.Fiber b → Aff c
  , teardown ∷ Aff.Fiber c → Aff Unit
  , stateVar ∷ AVar State
  }

foreign import data Throttler ∷ Type → Type

makeThrottler ∷ ∀ input b c . ThrottlerF input b c → Throttler input
makeThrottler = unsafeCoerce

withThrottler ∷ ∀ input r . (∀ b c . ThrottlerF input b c → r) → Throttler input → r
withThrottler f t = f (unsafeCoerce t)

new ∷
  ∀ input b c .
  (Aff.Fiber input → Aff b) →
  (Aff.Fiber b → Aff c) →
  (Aff.Fiber c → Aff Unit) →
  Aff (Throttler input)
new setup operation teardown =
  AVar.new initState <#> \stateVar →
    makeThrottler
      { setup
      , operation
      , teardown
      , stateVar
      }

kill ∷ ∀ input . Exception.Error → Throttler input → Aff Unit
kill error = withThrottler \ { stateVar } →
  AVar.kill error stateVar

modifyAVar ∷ ∀ a . AVar a → (a → a) → Aff Unit
modifyAVar avar f = AVar.take avar >>= \x → AVar.put (f x) avar

run ∷ ∀ input . Throttler input → Aff input → Aff (Aff.Fiber Unit)
run throttler input =
  throttler # withThrottler \ { setup, operation, teardown, stateVar } → do
    { queuedFiber, currentFiber } <- AVar.read stateVar
    case queuedFiber of
      Just queued → pure queued

      Nothing → do
        update <- Aff.suspendAff do
          setupFiber <- Aff.suspendAff do
             setup =<< Aff.forkAff input

          Aff.sequential $
            Aff.parallel (traverse_ Aff.joinFiber currentFiber) <*
            Aff.parallel (Aff.attempt $ Aff.joinFiber setupFiber)

          modifyAVar stateVar \state →
            state
              { queuedFiber = Nothing
              , currentFiber = state.queuedFiber
              }

          operationFiber <- Aff.suspendAff $ operation setupFiber
          outcome <- Aff.attempt $ Aff.joinFiber operationFiber
          modifyAVar stateVar _ { currentFiber = Nothing }
          teardown operationFiber

        modifyAVar stateVar _ { queuedFiber = Just update }
        Aff.forkAff $ Aff.joinFiber update

runSimple ∷ Throttler Unit → Aff (Aff.Fiber Unit)
runSimple t = run t (pure unit)

