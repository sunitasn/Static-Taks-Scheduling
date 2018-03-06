import java.util.*;
interface Printable
        { void print();}
class Item implements Printable
{
private String ItemName;
private double ItemPrice;
public double getItemPrice()
    {return ItemPrice;}
public String getItemName()
    {return ItemName;}
Item(String ItemName, double ItemPrice)
    { this.ItemName = ItemName;
      this.ItemPrice = ItemPrice;}
public void print()
    {System.out.println(ItemName + ItemPrice);}
} // end of item class
class Dish implements Printable
{
    private ArrayList Items = new ArrayList();
    void addItem(Item i)
    { Items.add(i);   }
   /* void removeItem(int i)
            {Items.remove(i);}
    void  setItem(Item i, int index)
            {Items.set(index, i);    }
    *
    */
    ArrayList getItems()
    { return Items;   }
    public void print()
    {
         Iterator itr = Items.iterator();
      while(itr.hasNext())
      {
          Item i = (Item) itr.next();
        System.out.println(i.getItemPrice() + i.getItemName());
      }
    }

            
/*    public double DishCost(int tax)
    { double k = 0;
        Iterator itr = Items.iterator();
      while(itr.hasNext())
        k += ((((Item)itr.next()).getItemPrice()*tax)/100);
        return k;
    }
*/
    } // end of dish class
class manager
{
    public static void main(String args[])
    {
    Dish NorthDish = new Dish();
    Dish SouthDish = new Dish();
    NorthDish.addItem(new Item("Rice",50.60));
    NorthDish.addItem(new Item("Sambhar",60.50));
    SouthDish.addItem(new Item("Chapti",50.60));
    SouthDish.addItem(new Item("Dal",60.50));
    //NorthDish.setItem(new Item("Dosa",40.30),0);
    Printable p = NorthDish;
    p.print();
    p= SouthDish;
    p.print();
    Comparator c = new 
            Comparator()
            {public int compare(Object o1, Object o2)
                { Item i = (Item) o1;
                  Item j = (Item) o2;
                  if(i.getItemPrice() == j.getItemPrice())
                      return 0;
                  else if(i.getItemPrice() > j.getItemPrice())
                      return 1;
                  else 
                      return -1;
              }
    };
    Collections.sort(NorthDish.getItems(),c);
    //System.out.print(NorthDish.DishCost(6))    ;
    }}


